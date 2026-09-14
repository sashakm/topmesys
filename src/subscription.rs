use std::{any::Any, fmt::Debug, panic::AssertUnwindSafe, sync::Arc};

use futures_util::{FutureExt, future::BoxFuture};
use tokio::{
    runtime::Handle,
    sync::mpsc::{
        self,
        error::{SendError, TrySendError},
    },
    task::JoinSet,
};

use crate::{
    DeadLetter, DeadLetterReason, DeadLetterSink, Delivery, DeliveryOutcome, EventConsumer,
    EventTopic, HandlerError, RetryPolicy, TopicError, type_states::Pattern,
};

/// What the broker does with a message when the inbox of a subscription it is routed to is full.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Overflow {
    /// Waits for the inbox to free up. Nothing is lost and emitters are slowed down to the pace of
    /// the subscription, but the broker routes no other message in the meantime: a subscription busy
    /// retrying stalls all others once its inbox is full, and handlers submitting events to the same
    /// broker can deadlock it.
    #[default]
    Block,
    /// Hands the message to the subscription's [DeadLetterSink] right away, so the subscription never
    /// stalls the broker.
    DeadLetter,
}

/// Configures one subscription of an [EventConsumer]: the topic pattern it matches and how messages
/// are delivered to it. Every subscription receives messages through its own bounded inbox, handles
/// up to [concurrency](Subscription::with_concurrency) of them at once, retries failed deliveries
/// according to its [RetryPolicy] and hands the messages it gives up on to its [DeadLetterSink].
/// Settings left unset fall back to the broker's.
#[derive(Debug, Clone)]
pub struct Subscription {
    pattern: String,
    retry_policy: Option<RetryPolicy>,
    dead_letter_sink: Option<Arc<dyn DeadLetterSink>>,
    inbox_capacity: Option<usize>,
    concurrency: Option<usize>,
    overflow: Overflow,
}

impl Subscription {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            retry_policy: None,
            dead_letter_sink: None,
            inbox_capacity: None,
            concurrency: None,
            overflow: Overflow::default(),
        }
    }

    /// Overrides the broker's [retry policy](crate::EventBroker::with_retry_policy).
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Overrides the broker's [dead letter sink](crate::EventBroker::with_dead_letter_sink).
    pub fn with_dead_letter_sink(mut self, sink: Arc<dyn DeadLetterSink>) -> Self {
        self.dead_letter_sink = Some(sink);
        self
    }

    /// How many messages the inbox holds before the [Overflow] policy applies. Defaults to the
    /// broker's buffer size, values below `1` are raised to `1`.
    pub fn with_inbox_capacity(mut self, capacity: usize) -> Self {
        self.inbox_capacity = Some(capacity);
        self
    }

    /// How many messages are handled at once, including deliveries waiting for a retry. With `1`,
    /// messages are handled strictly in order. Defaults to the inbox capacity, values below `1` are
    /// raised to `1`.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    pub fn with_overflow(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

impl From<&str> for Subscription {
    fn from(pattern: &str) -> Self {
        Self::new(pattern)
    }
}

impl From<String> for Subscription {
    fn from(pattern: String) -> Self {
        Self::new(pattern)
    }
}

/// The subscriptions of an [EventConsumer], each identified by a value of the consumer's
/// [Topic](EventConsumer::Topic). Consumers with a single subscription can convert a pattern or a
/// [Subscription] into `Subscriptions<()>`.
#[derive(Debug)]
pub struct Subscriptions<T> {
    entries: Vec<(T, Subscription)>,
}

impl<T> Subscriptions<T> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Adds a subscription, handing `topic` to [handle_event](EventConsumer::handle_event) along
    /// with every message it receives.
    pub fn on(mut self, topic: T, subscription: impl Into<Subscription>) -> Self {
        self.entries.push((topic, subscription.into()));
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = (&T, &Subscription)> {
        self.entries
            .iter()
            .map(|(topic, subscription)| (topic, subscription))
    }
}

impl<T> Default for Subscriptions<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&str> for Subscriptions<()> {
    fn from(pattern: &str) -> Self {
        Self::new().on((), pattern)
    }
}

impl From<String> for Subscriptions<()> {
    fn from(pattern: String) -> Self {
        Self::new().on((), pattern)
    }
}

impl From<Subscription> for Subscriptions<()> {
    fn from(subscription: Subscription) -> Self {
        Self::new().on((), subscription)
    }
}

/// Identifies a subscription registered with an [EventBroker](crate::EventBroker).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionInfo {
    pub(crate) pattern: EventTopic<Pattern>,
    pub(crate) consumer: &'static str,
}

impl SubscriptionInfo {
    pub fn pattern(&self) -> &EventTopic<Pattern> {
        &self.pattern
    }

    /// The type name of the consumer owning the subscription.
    pub fn consumer(&self) -> &'static str {
        self.consumer
    }
}

/// Calls a consumer's [handle_event](EventConsumer::handle_event) with the topic of one of its
/// subscriptions, hiding the consumer's type from the broker.
trait Handler: Send + Sync {
    fn handle<'a>(&'a self, delivery: &'a Delivery) -> BoxFuture<'a, Result<(), HandlerError>>;
}

struct TopicHandler<C: EventConsumer> {
    consumer: Arc<C>,
    topic: C::Topic,
}

impl<C: EventConsumer> Handler for TopicHandler<C> {
    fn handle<'a>(&'a self, delivery: &'a Delivery) -> BoxFuture<'a, Result<(), HandlerError>> {
        self.consumer.handle_event(&self.topic, delivery)
    }
}

/// Settings of the broker applying to subscriptions that don't configure their own.
#[derive(Debug, Clone, Default)]
pub(crate) struct Defaults {
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) dead_letter_sink: Option<Arc<dyn DeadLetterSink>>,
}

/// A subscription registered with a broker.
pub(crate) struct Subscriber {
    pub(crate) info: Arc<SubscriptionInfo>,
    handler: Box<dyn Handler>,
    config: Subscription,
}

impl Debug for Subscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscriber")
            .field("info", &self.info)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Subscriber {
    /// Creates a subscriber for each of the consumer's subscriptions. Fails without creating any if
    /// one of the patterns is invalid.
    pub(crate) fn from_consumer<C: EventConsumer>(
        consumer: C,
    ) -> Result<Vec<Arc<Self>>, TopicError> {
        let consumer = Arc::new(consumer);
        consumer
            .subscriptions()
            .entries
            .into_iter()
            .map(|(topic, config)| {
                Ok(Arc::new(Self {
                    info: Arc::new(SubscriptionInfo {
                        pattern: EventTopic::new(config.pattern.as_str()).as_subscription()?,
                        consumer: std::any::type_name::<C>(),
                    }),
                    handler: Box::new(TopicHandler {
                        consumer: consumer.clone(),
                        topic,
                    }),
                    config,
                }))
            })
            .collect()
    }

    /// Opens an inbox for the subscription and spawns the worker handling its deliveries into
    /// `workers`. The worker finishes once all senders of the inbox are dropped and it is empty.
    pub(crate) fn open(
        self: &Arc<Self>,
        inbox_capacity: usize,
        defaults: &Defaults,
        workers: &mut JoinSet<()>,
        runtime: &Handle,
    ) -> Inbox {
        let capacity = self.config.inbox_capacity.unwrap_or(inbox_capacity).max(1);
        let concurrency = self.config.concurrency.unwrap_or(capacity).max(1);
        let (sender, receiver) = mpsc::channel(capacity);
        let worker = Arc::new(Worker {
            subscriber: self.clone(),
            retry_policy: self
                .config
                .retry_policy
                .clone()
                .unwrap_or_else(|| defaults.retry_policy.clone()),
            dead_letter_sink: self
                .config
                .dead_letter_sink
                .clone()
                .or_else(|| defaults.dead_letter_sink.clone()),
        });
        workers.spawn_on(worker.clone().run(receiver, concurrency), runtime);
        Inbox { sender, worker }
    }
}

/// A subscriber in the broker's routing table, with its inbox while the broker is running.
#[derive(Debug)]
pub(crate) struct Route {
    pub(crate) subscriber: Arc<Subscriber>,
    pub(crate) inbox: Option<Inbox>,
}

#[derive(Debug, Clone)]
pub(crate) struct Inbox {
    sender: mpsc::Sender<Delivery>,
    worker: Arc<Worker>,
}

impl Inbox {
    pub(crate) fn subscription(&self) -> &Arc<SubscriptionInfo> {
        &self.worker.subscriber.info
    }

    /// Queues the delivery according to the subscription's [Overflow] policy. Deliveries that can't
    /// be queued are finished on `tasks`, so routing doesn't wait for dead letter sinks or transports.
    pub(crate) async fn deliver(&self, delivery: Delivery, tasks: &mut JoinSet<()>) {
        let rejected = match self.worker.subscriber.config.overflow {
            Overflow::Block => match self.sender.send(delivery).await {
                Ok(()) => return,
                Err(SendError(delivery)) => delivery,
            },
            Overflow::DeadLetter => match self.sender.try_send(delivery) {
                Ok(()) => return,
                Err(TrySendError::Full(delivery)) => {
                    let worker = self.worker.clone();
                    tasks.spawn(async move {
                        let outcome = worker
                            .dead_letter(&delivery, DeadLetterReason::InboxFull, 0)
                            .await;
                        delivery.finish(outcome).await;
                    });
                    return;
                }
                Err(TrySendError::Closed(delivery)) => delivery,
            },
        };
        // The inbox only closes while this sender is alive if the worker was aborted.
        tracing::error!(
            pattern = %self.subscription().pattern,
            consumer = self.subscription().consumer,
            "Subscription worker is gone, aborting event on `{}`",
            rejected.message.topic()
        );
        tasks.spawn(rejected.finish(DeliveryOutcome::Aborted));
    }
}

/// Handles the deliveries of one subscription.
#[derive(Debug)]
pub(crate) struct Worker {
    subscriber: Arc<Subscriber>,
    retry_policy: RetryPolicy,
    dead_letter_sink: Option<Arc<dyn DeadLetterSink>>,
}

impl Worker {
    /// Processes the inbox's deliveries, at most `concurrency` at a time, until it is closed and empty.
    async fn run(self: Arc<Self>, mut inbox: mpsc::Receiver<Delivery>, concurrency: usize) {
        let mut in_flight = JoinSet::new();
        loop {
            if in_flight.len() >= concurrency {
                if let Some(Err(e)) = in_flight.join_next().await {
                    tracing::error!("Event delivery task failed: {e}");
                }
                continue;
            }
            tokio::select! {
                biased;
                Some(result) = in_flight.join_next(), if !in_flight.is_empty() => {
                    if let Err(e) = result {
                        tracing::error!("Event delivery task failed: {e}");
                    }
                }
                maybe_delivery = inbox.recv() => match maybe_delivery {
                    Some(delivery) => {
                        in_flight.spawn(self.clone().process(delivery));
                    }
                    None => break,
                },
            }
        }
        while let Some(result) = in_flight.join_next().await {
            if let Err(e) = result {
                tracing::error!("Event delivery task failed: {e}");
            }
        }
    }

    /// Calls the handler until it succeeds, fails permanently or runs out of retries, then finishes
    /// the delivery. A panicking handler counts as a permanent failure.
    async fn process(self: Arc<Self>, mut delivery: Delivery) {
        let outcome = loop {
            let error = match AssertUnwindSafe(self.subscriber.handler.handle(&delivery))
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => break DeliveryOutcome::Handled,
                Ok(Err(error)) => error,
                Err(panic) => HandlerError::permanent(anyhow::anyhow!(
                    "event handler panicked: {}",
                    panic_message(panic.as_ref())
                )),
            };
            if error.is_permanent() || delivery.attempt > self.retry_policy.retries() {
                let attempts = delivery.attempt;
                break self
                    .dead_letter(&delivery, DeadLetterReason::HandlerFailed(error), attempts)
                    .await;
            }
            let delay = self.retry_policy.jittered_delay(delivery.attempt);
            tracing::warn!(
                pattern = %self.subscriber.info.pattern,
                consumer = self.subscriber.info.consumer,
                attempt = delivery.attempt,
                ?delay,
                "Retrying event on `{}`: {error:#}",
                delivery.message.topic()
            );
            delivery.attempt = delivery.attempt.saturating_add(1);
            if let Some(handle) = delivery.message.transport_handle()
                && let Err(e) = handle.on_retry(delivery.attempt, delay).await
            {
                tracing::warn!(
                    "Transport retry hook failed for event on `{}`: {e:#}",
                    delivery.message.topic()
                );
            }
            tokio::time::sleep(delay).await;
        };
        delivery.finish(outcome).await;
    }

    /// Hands the delivery's message to the dead letter sink, or logs and drops it without one.
    async fn dead_letter(
        &self,
        delivery: &Delivery,
        reason: DeadLetterReason,
        attempts: u32,
    ) -> DeliveryOutcome {
        let info = &self.subscriber.info;
        let topic = delivery.message.topic();
        let Some(sink) = &self.dead_letter_sink else {
            tracing::error!(
                pattern = %info.pattern,
                consumer = info.consumer,
                attempts,
                "Dropping event on `{topic}`: {reason:#}"
            );
            return DeliveryOutcome::Failed;
        };
        tracing::warn!(
            pattern = %info.pattern,
            consumer = info.consumer,
            attempts,
            "Dead-lettering event on `{topic}`: {reason:#}"
        );
        let letter = DeadLetter {
            message: delivery.message.clone(),
            subscription: info.clone(),
            reason,
            attempts,
        };
        match sink.dead_letter(letter).await {
            Ok(()) => DeliveryOutcome::DeadLettered,
            Err(e) => {
                tracing::error!(
                    pattern = %info.pattern,
                    consumer = info.consumer,
                    "Dead letter sink failed for event on `{topic}`: {e:#}"
                );
                DeliveryOutcome::Failed
            }
        }
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}
