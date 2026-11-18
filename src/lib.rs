use std::{
    any::Any,
    collections::BTreeMap,
    fmt::Display,
    marker::PhantomData,
    sync::{Arc, PoisonError},
};

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{StreamExt, future::select};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_stream::wrappers::ReceiverStream;
use type_states::{Init, Pattern, RoutingKey, Running, StateMarker, Stopped};

mod dead_letter;
mod delivery;
mod retry;
mod subscription;
mod transport;

pub use dead_letter::{DeadLetter, DeadLetterReason, DeadLetterSink};
pub use delivery::{Delivery, HandlerError};
pub use retry::{Backoff, RetryPolicy};
pub use subscription::{Overflow, Subscription, SubscriptionInfo, Subscriptions};
pub use transport::{DeliveryOutcome, Settlement, SubscriptionOutcome, TransportHandle};

use subscription::{Defaults, Inbox, Route, Subscriber};
use transport::Settler;

type EventSubscriptions = Arc<DashMap<EventTopic<Pattern>, Vec<Route>>>;

/// Allows for a concise representation and implementation of the [EventBroker]s and the [EventTopic]s
/// lifecycle modes
pub mod type_states {
    // Used by EventTopic
    #[derive(Debug, Clone)]
    pub struct Init;
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    pub struct RoutingKey;
    #[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
    pub struct Pattern;
    // Used by EventBroker
    #[derive(Clone, Debug)]
    pub struct Stopped {
        pub(super) bufsize: usize,
    }
    #[derive(Debug)]
    pub struct Running {
        pub(super) message_tx: super::mpsc::Sender<super::EventMessage>,
        pub(super) stop_tx: super::watch::Sender<bool>,
        pub(super) handle: tokio::task::JoinHandle<()>,
        pub(super) runtime: tokio::runtime::Handle,
        pub(super) workers: std::sync::Mutex<super::JoinSet<()>>,
    }

    pub trait StateMarker {}
    impl StateMarker for () {}
    impl StateMarker for Init {}
    impl StateMarker for RoutingKey {}
    impl StateMarker for Pattern {}

    impl StateMarker for Stopped {}
    impl StateMarker for Running {}
}

/// Errors that can occur when parsing and validating [EventTopic]s.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicError {
    #[error("topic cannot be empty")]
    EmptyTopic,
    #[error(
        "invalid character in topic `{topic}`: topics may only contain alphanumeric characters and {allowed:?}"
    )]
    InvalidCharacter {
        topic: String,
        allowed: &'static [char],
    },
    #[error("found empty segment")]
    EmptySegment,
    #[error("invalid selection segment `{0}`")]
    InvalidSelection(String),
    #[error("invalid value `{value}` in selection segment `{segment}`")]
    InvalidSelectionValue { segment: String, value: String },
    #[error("found `,` outside of a selection segment: `{0}`")]
    UnbracketedComma(String),
    #[error("`*` must make up the entire segment: `{0}`")]
    EmbeddedWildcard(String),
}

/// Errors that can occur when operating an [EventBroker].
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("no tokio runtime available to run the event loop")]
    Runtime(#[from] tokio::runtime::TryCurrentError),
}

/// A type representing a single topic segment. Topic segments are used to match against other topic segments. Literal segments
/// matched against other literal segments must be equal. Selection segments are matched against literal segments by checking if the literal segment
/// value is contained within the selection segment. Wildcard segments match against any segment. Selection segments are defined by square brackets and comma separated values.
/// When parsing selection segments, and inside the selection a wildcard segment is found, the entire selection will be parsed as wildcard. If a selection segment contains only a single
/// value, the segment will be parsed as literal. Multiple values will be sorted and deduplicated, so selection segment lookups should be fairly quick.
/// Wildcard segments are defined by a single asterisk. Literal segments are any other string. Selection values must be non-empty and may not
/// contain brackets or embedded wildcards; segments that could never match a routing key (like `[a,]` or `a[b]`) are rejected when parsed.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TopicSegment {
    Literal(String),
    Wildcard,
    Selection(Vec<String>),
}

impl TopicSegment {
    fn matches(&self, other: &TopicSegment) -> bool {
        match (self, other) {
            (TopicSegment::Literal(l), TopicSegment::Literal(r)) => l == r,
            (TopicSegment::Selection(l), TopicSegment::Literal(r)) => l.binary_search(r).is_ok(),
            (TopicSegment::Wildcard, _) => true,
            _ => false,
        }
    }

    fn parse(text: &str) -> Result<Self, TopicError> {
        if text.is_empty() {
            return Err(TopicError::EmptySegment);
        }
        if text == "*" {
            return Ok(Self::Wildcard);
        }
        if text.starts_with('[') && text.ends_with(']') {
            let mut values = Vec::new();
            for value in text[1..text.len() - 1].split(',').map(str::trim) {
                if value == "*" {
                    return Ok(Self::Wildcard);
                }
                if value.is_empty() || value.contains(['[', ']', '*']) {
                    return Err(TopicError::InvalidSelectionValue {
                        segment: text.to_string(),
                        value: value.to_string(),
                    });
                }
                values.push(value.to_string());
            }
            values.sort();
            values.dedup();
            if values.len() == 1 {
                return Ok(Self::Literal(values.pop().expect("one value is present")));
            }
            return Ok(Self::Selection(values));
        }
        if text.contains(['[', ']']) {
            return Err(TopicError::InvalidSelection(text.to_string()));
        }
        if text.contains(',') {
            return Err(TopicError::UnbracketedComma(text.to_string()));
        }
        if text.contains('*') {
            return Err(TopicError::EmbeddedWildcard(text.to_string()));
        }
        Ok(Self::Literal(text.to_string()))
    }
}

/// A type representing an events context. [EventTopic]s are generated from strings and used as either subscription patterns
/// or routing keys for event messages. Periods are used to create [TopicSegment]s within a topic string to allow further categorisation
/// and structural representation. A topic intended as a routing key only permits strings of alphanumeric characters and
/// the characters `.` `-` `_`, ensuring it consists only of literal segments which can be used to match subscription pattern topics.
/// Setting up the topic as a subscription pattern allows topics to consist of alphanumeric characters as well as `.`, `-`, `_`, `[`, `]`, `,`, `*`
/// enabling literal, wildcard (`*`) and selection (`[val1,val2,valN]`) segments. This will also enable tail matching if a wildcard segment is found at the end of the topic.
/// If many wildcard segments are found at the end of the pattern, one will be kept and the rest discarded, as it won't affect matching. A topic
/// consisting of a single wildcard segment matches any other topic.
///
/// Topic strings are lowercased when the topic is created, making topic matching case-insensitive: `Orders.EU` and `orders.eu` are the same topic.
#[derive(Debug, Default, Clone, Hash, PartialEq, Eq)]
pub struct EventTopic<S: StateMarker> {
    s: PhantomData<S>,
    raw: String,
    segments: Vec<TopicSegment>,
    segment_count: usize,
    is_wildcard: bool,
    is_tail_matching: bool,
}

impl EventTopic<()> {
    pub fn new(topic: impl Into<String>) -> EventTopic<Init> {
        EventTopic {
            s: PhantomData::<Init>,
            raw: topic.into().to_lowercase(),
            segments: Vec::new(),
            segment_count: 0,
            is_wildcard: false,
            is_tail_matching: false,
        }
    }
}

impl EventTopic<Init> {
    pub fn as_routing_key(self) -> Result<EventTopic<RoutingKey>, TopicError> {
        self.sanitize_topic(&['.', '-', '_'])?;
        let segments = self.parse_segments()?;
        let new = EventTopic {
            s: PhantomData::<RoutingKey>,
            raw: self.raw.clone(),
            segment_count: segments.len(),
            segments,
            is_tail_matching: false,
            is_wildcard: false,
        };
        Ok(new)
    }

    pub fn as_subscription(self) -> Result<EventTopic<Pattern>, TopicError> {
        self.sanitize_topic(&['.', ',', '*', '[', ']', '-', '_'])?;
        let mut new = EventTopic {
            s: PhantomData::<Pattern>,
            raw: self.raw.clone(),
            ..Default::default()
        };
        let segments = self.parse_segments()?;
        if let Some(TopicSegment::Wildcard) = segments.last() {
            new.is_tail_matching = true;
            if segments.len() == 1 {
                new.is_wildcard = true;
            } else {
                let tail_wildcards = segments
                    .iter()
                    .rev()
                    .take_while(|s| *s == &TopicSegment::Wildcard)
                    .count();
                let real_segments = segments.len() - tail_wildcards + 1;
                new.segments = segments.into_iter().take(real_segments).fold(
                    new.segments,
                    |mut acc, segment| {
                        acc.push(segment);
                        acc
                    },
                );
            }
        } else {
            new.segments = segments;
        }
        new.segment_count = new.segments.len();
        Ok(new)
    }

    fn parse_segments(&self) -> Result<Vec<TopicSegment>, TopicError> {
        let mut parsed_segments = Vec::new();
        for segment in self.raw.split('.') {
            parsed_segments.push(TopicSegment::parse(segment)?);
        }
        Ok(parsed_segments)
    }

    fn sanitize_topic(&self, extra_keys: &'static [char]) -> Result<(), TopicError> {
        if self.raw.is_empty() {
            return Err(TopicError::EmptyTopic);
        }
        if !self
            .raw
            .chars()
            .all(|c| c.is_alphanumeric() || extra_keys.contains(&c))
        {
            return Err(TopicError::InvalidCharacter {
                topic: self.raw.clone(),
                allowed: extra_keys,
            });
        }
        Ok(())
    }
}

impl EventTopic<Pattern> {
    #[tracing::instrument(level = "debug")]
    fn match_topic(&self, topic: &EventTopic<RoutingKey>) -> bool {
        if self.is_wildcard {
            true
        } else if self.segment_count <= topic.segment_count && self.is_tail_matching {
            let take = self.segment_count - 1;
            self.segments
                .iter()
                .take(take)
                .zip(topic.segments.iter().take(take))
                .all(|(a, b)| a.matches(b))
        } else if self.segment_count == topic.segment_count {
            self.segments
                .iter()
                .zip(topic.segments.iter())
                .all(|(a, b)| a.matches(b))
        } else {
            false
        }
    }
}

impl<S> EventTopic<S>
where
    S: StateMarker,
{
    pub fn text(&self) -> &str {
        &self.raw
    }

    pub fn segments(&self) -> &[TopicSegment] {
        &self.segments
    }
}

impl<S> Display for EventTopic<S>
where
    S: StateMarker,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

/// Represents either a single or multiple [EventMessage]s and enables [EventEmitter] implementors
/// to submit batched events to an [EventBroker].
#[derive(Debug, PartialEq, Eq)]
pub enum EventSubmission {
    Single(EventMessage),
    Batch(Vec<EventMessage>),
}

impl From<EventMessage> for EventSubmission {
    fn from(value: EventMessage) -> Self {
        Self::Single(value)
    }
}

/// Clones the message, which leaves its [TransportHandle] behind.
impl From<&EventMessage> for EventSubmission {
    fn from(value: &EventMessage) -> Self {
        Self::Single(value.clone())
    }
}

impl FromIterator<EventMessage> for EventSubmission {
    fn from_iter<T: IntoIterator<Item = EventMessage>>(iter: T) -> Self {
        Self::Batch(Vec::from_iter(iter))
    }
}

/// Enables implementors to submit one or more [EventMessage]s to a [channel](mpsc::Sender) connected
/// to an [EventBroker]. When overriding the [submit_event](EventEmitter::submit_event) default implementation,
/// implementors should ensure that cancel safety is maintained by using the channels [reserve](mpsc::Sender::reserve)
/// method. Under normal circumstances, the EventBroker will handle any outstanding [permits](tokio::sync::mpsc::Permit)
/// when being shut down.
#[async_trait::async_trait]
pub trait EventEmitter: std::fmt::Debug {
    fn get_sender(&self) -> &mpsc::Sender<EventMessage>;

    async fn submit_event(&self, submission: EventSubmission) -> anyhow::Result<()> {
        match submission {
            EventSubmission::Single(event_message) => {
                self.get_sender().reserve().await?.send(event_message)
            }
            EventSubmission::Batch(event_messages) => {
                self.get_sender()
                    .reserve_many(event_messages.len())
                    .await?
                    .zip(event_messages)
                    .for_each(|(permit, msg)| permit.send(msg));
            }
        }
        Ok(())
    }
}

/// Implementors are registered with an [EventBroker] and receive [EventMessage]s on one or more
/// [Subscriptions]. Every subscription pairs a topic pattern with a value of the consumer's
/// [Topic](EventConsumer::Topic), which is handed to [handle_event](EventConsumer::handle_event)
/// along with each [Delivery] it receives, so messages can be handled per subscription. A message
/// matching several subscriptions of a consumer is delivered once on each of them.
///
/// Errors returned by the handler are retried according to the subscription's [RetryPolicy], and
/// messages the subscription gives up on are handed to its [DeadLetterSink], see [HandlerError].
/// #### Example
/// ```
/// use std::time::Duration;
/// use topmesys::{Delivery, EventConsumer, HandlerError, RetryPolicy, Subscription, Subscriptions};
///
/// #[derive(Debug)]
/// enum NoteTopic {
///     Created,
///     Archived,
/// }
///
/// #[derive(Debug)]
/// struct AuditLog;
///
/// #[async_trait::async_trait]
/// impl EventConsumer for AuditLog {
///     type Topic = NoteTopic;
///
///     fn subscriptions(&self) -> Subscriptions<NoteTopic> {
///         Subscriptions::new()
///             .on(NoteTopic::Created, "notes.*.created")
///             .on(
///                 NoteTopic::Archived,
///                 Subscription::new("notes.*.archived")
///                     .with_retry_policy(RetryPolicy::exponential(5, Duration::from_millis(100))),
///             )
///     }
///
///     async fn handle_event(
///         &self,
///         topic: &NoteTopic,
///         delivery: &Delivery,
///     ) -> Result<(), HandlerError> {
///         let note = std::str::from_utf8(delivery.message().content())?;
///         match topic {
///             NoteTopic::Created => println!("created {note}"),
///             NoteTopic::Archived => println!("archived {note}"),
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait EventConsumer: std::fmt::Debug + Send + Sync + 'static {
    /// Identifies the consumer's subscriptions, typically an enum. Consumers with a single
    /// subscription can use `()`.
    type Topic: std::fmt::Debug + Send + Sync + 'static;

    /// Called once, when the consumer is registered with a broker.
    fn subscriptions(&self) -> Subscriptions<Self::Topic>;

    async fn handle_event(
        &self,
        topic: &Self::Topic,
        delivery: &Delivery,
    ) -> Result<(), HandlerError>;
}

/// Identifies the subscriptions of an [EventConsumer] registered with an [EventBroker]. Returned by
/// [add_topic_consumer](EventBroker::add_topic_consumer) and consumed by
/// [remove_topic_consumer](EventBroker::remove_topic_consumer) to unsubscribe the consumer again.
/// The handle holds [Weak](std::sync::Weak) references, so keeping it around does not keep the
/// consumer alive.
#[derive(Debug, Clone)]
pub struct SubscriptionHandle {
    subscriptions: Vec<(EventTopic<Pattern>, std::sync::Weak<Subscriber>)>,
}

/// This is the central bus for all events in the application. It sets up a [channel](tokio::sync::mpsc) and routes
/// received [EventMessage]s to the [Subscription]s of [EventConsumer]s with matching topic patterns. Every subscription
/// has its own bounded inbox and worker, which calls the consumer's [handle_event](EventConsumer::handle_event) method,
/// retries failed deliveries according to the subscription's [RetryPolicy] and hands messages it gives up on to a
/// [DeadLetterSink]. When an inbox is full, the subscription's [Overflow] policy decides whether routing waits or the
/// message is dead-lettered. Messages whose topic doesn't match any subscription are dropped silently, and messages
/// carrying a [TransportHandle] are settled once all of their deliveries finished.
/// Messages are submitted to the channel using a [`Sender<EventMessage>`](mpsc::Sender), produced by the [get_sender](EventBroker::get_sender) method.
/// While the sender can be used for submissions "as is", the [submit_event][EventEmitter::submit_event] from the [EventEmitter] trait provides a safe default
/// implementation using [channel permits](tokio::sync::mpsc::Permit), conveniently supporting both single and batched [EventSubmission]s.
#[derive(Debug)]
pub struct EventBroker<S: StateMarker> {
    runtime: S,
    subscriptions: EventSubscriptions,
    handle_ctrl_c: bool,
    defaults: Defaults,
}

impl Default for EventBroker<Stopped> {
    fn default() -> EventBroker<Stopped> {
        EventBroker::new(64)
    }
}

impl EventBroker<()> {
    pub fn new(bufsize: usize) -> EventBroker<Stopped> {
        EventBroker {
            runtime: Stopped { bufsize },
            subscriptions: Arc::new(DashMap::new()),
            handle_ctrl_c: false,
            defaults: Defaults::default(),
        }
    }
}

impl<S> EventBroker<S>
where
    S: StateMarker + 'static,
{
    fn register(
        &self,
        subscribers: Vec<Arc<Subscriber>>,
        mut open: impl FnMut(&Arc<Subscriber>) -> Option<Inbox>,
    ) -> SubscriptionHandle {
        let mut handle = SubscriptionHandle {
            subscriptions: Vec::with_capacity(subscribers.len()),
        };
        for subscriber in subscribers {
            let pattern = subscriber.info.pattern().clone();
            handle
                .subscriptions
                .push((pattern.clone(), Arc::downgrade(&subscriber)));
            let inbox = open(&subscriber);
            self.subscriptions
                .entry(pattern)
                .or_default()
                .push(Route { subscriber, inbox });
        }
        handle
    }

    /// Removes the subscriptions identified by the given [SubscriptionHandle]. Messages already queued
    /// for them are still handled. Returns `true` if any subscription was found and removed, `false`
    /// if they were removed before.
    pub fn remove_topic_consumer(&self, handle: SubscriptionHandle) -> bool {
        let mut removed = false;
        for (pattern, subscriber) in handle.subscriptions {
            let Some(subscriber) = subscriber.upgrade() else {
                continue;
            };
            if let Some(mut routes) = self.subscriptions.get_mut(&pattern) {
                let before = routes.len();
                routes.retain(|route| !Arc::ptr_eq(&route.subscriber, &subscriber));
                removed |= routes.len() < before;
            }
            self.subscriptions
                .remove_if(&pattern, |_, routes| routes.is_empty());
        }
        removed
    }

    /// All subscriptions registered with the broker.
    pub fn subscriptions(&self) -> Vec<Arc<SubscriptionInfo>> {
        self.subscriptions
            .iter()
            .flat_map(|entry| {
                entry
                    .value()
                    .iter()
                    .map(|route| route.subscriber.info.clone())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The subscriptions a message with the given topic is delivered to.
    pub fn find_subscriptions(&self, topic: &EventTopic<RoutingKey>) -> Vec<Arc<SubscriptionInfo>> {
        self.matching_routes(topic, |route| Some(route.subscriber.info.clone()))
    }

    fn matching_routes<T>(
        &self,
        topic: &EventTopic<RoutingKey>,
        pick: impl Fn(&Route) -> Option<T>,
    ) -> Vec<T> {
        let mut picked = Vec::new();
        for entry in self.subscriptions.iter() {
            if entry.key().match_topic(topic) {
                picked.extend(entry.value().iter().filter_map(&pick));
            }
        }
        picked
    }

    #[tracing::instrument(skip(self))]
    async fn run_event_loop(
        &self,
        receiver: mpsc::Receiver<EventMessage>,
        mut stop_rx: watch::Receiver<bool>,
    ) {
        let handle_ctrl_c = self.handle_ctrl_c;
        let stop_signal = async move {
            if handle_ctrl_c {
                if let Err(e) = tokio::signal::ctrl_c().await {
                    tracing::error!("Failed to listen for the ctrl-c signal: {e}");
                    std::future::pending::<()>().await
                }
            } else {
                std::future::pending::<()>().await
            }
        };
        let stop_call = stop_rx.changed();
        let mut stop = select(Box::pin(stop_signal), Box::pin(stop_call));
        let mut event_stream = ReceiverStream::new(receiver);
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut stop => {
                    tracing::info!("Stopping event stream processing.");
                    event_stream.close();
                    break;
                }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(e) = result {
                        tracing::error!("Event dispatch task failed: {e}");
                    }
                }
                maybe_msg = event_stream.next() => match maybe_msg {
                    Some(event_msg) => self.dispatch(event_msg, &mut tasks).await,
                    None => {
                        tracing::info!("Event loop processing ended. Shutting down broker.");
                        break;
                    }
                },
            }
        }
        // Route events still buffered in the channel, then close the inboxes, so every subscription
        // finishes once it processed its queued deliveries and no accepted message is lost.
        while let Some(event_msg) = event_stream.next().await {
            self.dispatch(event_msg, &mut tasks).await;
        }
        close_inboxes(&self.subscriptions);
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!("Event dispatch task failed: {e}");
            }
        }
    }

    /// Queues a delivery of the message in the inbox of every matching subscription.
    async fn dispatch(&self, event_msg: EventMessage, tasks: &mut JoinSet<()>) {
        let inboxes = self.matching_routes(event_msg.topic(), |route| route.inbox.clone());
        let message = Arc::new(event_msg);
        let settler = Settler::new(&message, inboxes.len());
        let Some(last) = inboxes.len().checked_sub(1) else {
            if let Some(settler) = settler {
                tasks.spawn(settler.settle());
            }
            return;
        };
        let mut settler = settler.map(Arc::new);
        for (index, inbox) in inboxes.into_iter().enumerate() {
            // The last delivery takes over the dispatcher's reference, so only deliveries hold the
            // settler and the one finishing last settles the message.
            let shared_settler = if index == last {
                settler.take()
            } else {
                settler.clone()
            };
            let delivery = Delivery {
                message: message.clone(),
                subscription: inbox.subscription().clone(),
                attempt: 1,
                settler: shared_settler,
            };
            inbox.deliver(delivery, tasks).await;
        }
    }
}

/// Drops the broker's inbox senders, so every subscription worker finishes once its inbox is empty.
fn close_inboxes(subscriptions: &EventSubscriptions) {
    for mut routes in subscriptions.iter_mut() {
        for route in routes.iter_mut() {
            route.inbox = None;
        }
    }
}

impl EventBroker<Stopped> {
    /// Additionally stops the running broker when the process receives a ctrl-c signal. This is
    /// opt-in, as listening for process signals from within a library may conflict with the
    /// embedding application's own signal handling.
    pub fn with_ctrl_c_handling(mut self) -> Self {
        self.handle_ctrl_c = true;
        self
    }

    /// Sets the retry policy of subscriptions that don't configure their own. Defaults to
    /// [RetryPolicy::none].
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.defaults.retry_policy = policy;
        self
    }

    /// Sets the dead letter sink of subscriptions that don't configure their own. Without a sink,
    /// messages subscriptions give up on are logged and dropped.
    pub fn with_dead_letter_sink(mut self, sink: Arc<dyn DeadLetterSink>) -> Self {
        self.defaults.dead_letter_sink = Some(sink);
        self
    }

    /// Registers the consumer's subscriptions, which receive messages once the broker runs.
    pub fn add_topic_consumer(
        &self,
        consumer: impl EventConsumer,
    ) -> Result<SubscriptionHandle, TopicError> {
        Ok(self.register(Subscriber::from_consumer(consumer)?, |_| None))
    }

    pub fn run(self) -> Result<EventBroker<Running>, BrokerError> {
        let rt = tokio::runtime::Handle::try_current()?;
        let (message_tx, message_rx) = mpsc::channel::<EventMessage>(self.runtime.bufsize);
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut workers = JoinSet::new();
        for mut routes in self.subscriptions.iter_mut() {
            for route in routes.iter_mut() {
                route.inbox = Some(route.subscriber.open(
                    self.runtime.bufsize,
                    &self.defaults,
                    &mut workers,
                    &rt,
                ));
            }
        }
        let broker = EventBroker {
            subscriptions: self.subscriptions.clone(),
            handle_ctrl_c: self.handle_ctrl_c,
            defaults: self.defaults.clone(),
            runtime: Running {
                handle: rt.spawn(async move { self.run_event_loop(message_rx, stop_rx).await }),
                message_tx,
                stop_tx,
                runtime: rt,
                workers: std::sync::Mutex::new(workers),
            },
        };

        Ok(broker)
    }
}

impl EventBroker<Running> {
    /// Registers the consumer's subscriptions, which receive messages right away.
    pub fn add_topic_consumer(
        &self,
        consumer: impl EventConsumer,
    ) -> Result<SubscriptionHandle, TopicError> {
        let subscribers = Subscriber::from_consumer(consumer)?;
        let mut workers = self
            .runtime
            .workers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Clean up after the workers of removed subscriptions.
        while let Some(result) = workers.try_join_next() {
            if let Err(e) = result {
                tracing::error!("Subscription worker failed: {e}");
            }
        }
        let inbox_capacity = self.runtime.message_tx.max_capacity();
        Ok(self.register(subscribers, |subscriber| {
            Some(subscriber.open(
                inbox_capacity,
                &self.defaults,
                &mut workers,
                &self.runtime.runtime,
            ))
        }))
    }

    /// Stops the broker once every accepted message was handled, dead-lettered or dropped, including
    /// deliveries still waiting for a retry.
    pub async fn stop(self) -> EventBroker<Stopped> {
        match self.runtime.stop_tx.send(true) {
            Err(e) => {
                tracing::error!("Failed to send stop signal to event loop: {e}");
                self.runtime.handle.abort();
            }
            Ok(_) => {
                if let Err(e) = self.runtime.handle.await {
                    tracing::error!("Event loop task failed to stop gracefully: {e}.");
                }
            }
        }
        // Subscriptions added after the event loop ended still have open inboxes.
        close_inboxes(&self.subscriptions);
        let mut workers = self
            .runtime
            .workers
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        while let Some(result) = workers.join_next().await {
            if let Err(e) = result {
                tracing::error!("Subscription worker failed: {e}");
            }
        }
        EventBroker {
            runtime: Stopped {
                bufsize: self.runtime.message_tx.max_capacity(),
            },
            subscriptions: self.subscriptions.clone(),
            handle_ctrl_c: self.handle_ctrl_c,
            defaults: self.defaults,
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<EventMessage> {
        self.runtime.message_tx.clone()
    }
}

/// A type representing an event handled by the applications event bus, consisting
/// of a [Into] [String] subject, [Into] [Bytes] content and string headers.
///
/// Headers carry what is about the message rather than in it: an event id, a
/// correlation id, a W3C `traceparent`. The broker never reads them; they travel with
/// the message to every consumer and dead letter sink, so a bridge to another messaging
/// system can map them onto that system's own headers.
///
/// Messages received from another messaging system can additionally carry a [TransportHandle],
/// which the broker settles once the message was delivered. Clones and comparisons take topic,
/// content and headers into account, but not the handle: a clone leaves it behind, so only the
/// original settles it.
/// #### Example
/// ```
/// use topmesys::{EventMessage, EventTopic};
///
/// let content = vec![123, 34, 116, 111, 34, 58, 34, 116, 104, 101, 109, 34, 125];
///
/// let first_msg = EventMessage::new("my-message", content)
///     .unwrap()
///     .with_header("correlation-id", "c-42");
/// let second_msg = EventMessage::default()
///     .with_topic(EventTopic::new("my-message").as_routing_key().unwrap())
///     .with_content(r#"{"to":"them"}"#);
///
/// assert_eq!(first_msg.topic(), second_msg.topic());
/// assert_eq!(first_msg.content(), second_msg.content());
/// assert_eq!(first_msg.header("correlation-id"), Some("c-42"));
/// assert_eq!(second_msg.header("correlation-id"), None);
/// ```
#[derive(Debug, Default)]
pub struct EventMessage {
    topic: EventTopic<RoutingKey>,
    content: Bytes,
    headers: BTreeMap<String, String>,
    transport: Option<Box<dyn TransportHandle>>,
}

impl Clone for EventMessage {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            content: self.content.clone(),
            headers: self.headers.clone(),
            transport: None,
        }
    }
}

impl PartialEq for EventMessage {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.content == other.content && self.headers == other.headers
    }
}

impl Eq for EventMessage {}

impl EventMessage {
    pub fn new(
        topic_text: impl Into<String>,
        content: impl Into<Bytes>,
    ) -> Result<Self, TopicError> {
        Ok(Self {
            topic: EventTopic::new(topic_text.into()).as_routing_key()?,
            content: content.into(),
            headers: BTreeMap::new(),
            transport: None,
        })
    }

    pub fn topic(&self) -> &EventTopic<RoutingKey> {
        &self.topic
    }

    pub fn content(&self) -> &Bytes {
        &self.content
    }

    /// The value of header `name`, if the message carries it.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// Every header, ordered by name.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    /// The message's transport handle, if it carries one of type `T`.
    pub fn transport<T: TransportHandle>(&self) -> Option<&T> {
        let handle: &dyn Any = self.transport.as_deref()?;
        handle.downcast_ref()
    }

    /// The message's transport handle, regardless of its type.
    pub fn transport_handle(&self) -> Option<&dyn TransportHandle> {
        self.transport.as_deref()
    }

    /// Self-consuming topic setter
    pub fn with_topic(mut self, topic: EventTopic<RoutingKey>) -> Self {
        self.topic = topic;
        self
    }

    /// Self-consuming content setter
    pub fn with_content(mut self, content: impl Into<Bytes>) -> Self {
        self.content = content.into();
        self
    }

    /// Self-consuming header setter. Setting a header again replaces its value.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Self-consuming setter for several headers at once.
    pub fn with_headers(
        mut self,
        headers: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.headers.extend(
            headers
                .into_iter()
                .map(|(name, value)| (name.into(), value.into())),
        );
        self
    }

    /// Self-consuming transport handle setter
    pub fn with_transport(mut self, handle: impl TransportHandle) -> Self {
        self.transport = Some(Box::new(handle));
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use dashmap::DashMap;
    use tokio::sync::{Semaphore, mpsc};

    use super::{
        DeadLetter, DeadLetterReason, DeadLetterSink, Delivery, DeliveryOutcome, EventBroker,
        EventConsumer, EventEmitter, EventMessage, EventSubmission, EventTopic, HandlerError,
        Overflow, RetryPolicy, Settlement, Subscription, Subscriptions, TopicSegment,
        TransportHandle,
    };

    /// A call of a [ScriptedConsumer]'s handler.
    #[derive(Debug, Clone)]
    struct Call {
        content: String,
        attempt: u32,
        transport: Option<&'static str>,
        headers: std::collections::BTreeMap<String, String>,
    }

    type Calls = Arc<Mutex<Vec<Call>>>;

    #[derive(Debug, Clone, Copy)]
    enum Script {
        Succeed,
        FailAttempts(u32),
        FailPermanently,
        Panic,
    }

    /// Consumer following a [Script] and recording every call of its handler.
    #[derive(Debug)]
    struct ScriptedConsumer {
        subscription: Subscription,
        script: Script,
        calls: Calls,
    }

    impl ScriptedConsumer {
        fn new(subscription: impl Into<Subscription>, script: Script) -> (Self, Calls) {
            let calls = Calls::default();
            let consumer = Self {
                subscription: subscription.into(),
                script,
                calls: calls.clone(),
            };
            (consumer, calls)
        }
    }

    #[async_trait::async_trait]
    impl EventConsumer for ScriptedConsumer {
        type Topic = ();

        fn subscriptions(&self) -> Subscriptions<()> {
            self.subscription.clone().into()
        }

        async fn handle_event(&self, _topic: &(), delivery: &Delivery) -> Result<(), HandlerError> {
            let call = Call {
                content: String::from_utf8(delivery.message().content().to_vec())?,
                attempt: delivery.attempt(),
                transport: delivery
                    .transport::<RecordingHandle>()
                    .map(|handle| handle.id),
                headers: delivery.message().headers().clone(),
            };
            self.calls.lock().unwrap().push(call);
            match self.script {
                Script::FailAttempts(failing) if delivery.attempt() <= failing => {
                    Err(anyhow::anyhow!("attempt {} failed", delivery.attempt()).into())
                }
                Script::Succeed | Script::FailAttempts(_) => Ok(()),
                Script::FailPermanently => Err(HandlerError::permanent(anyhow::anyhow!(
                    "malformed payload"
                ))),
                Script::Panic => panic!("handler bug"),
            }
        }
    }

    type Settlements = Arc<Mutex<Vec<(&'static str, Settlement)>>>;

    /// Transport handle recording its settlement and retry notifications.
    #[derive(Debug)]
    struct RecordingHandle {
        id: &'static str,
        settlements: Settlements,
        retries: Arc<AtomicUsize>,
    }

    impl RecordingHandle {
        fn new(id: &'static str, settlements: &Settlements, retries: &Arc<AtomicUsize>) -> Self {
            Self {
                id,
                settlements: settlements.clone(),
                retries: retries.clone(),
            }
        }
    }

    #[async_trait::async_trait]
    impl TransportHandle for RecordingHandle {
        async fn settle(&self, settlement: &Settlement) -> anyhow::Result<()> {
            self.settlements
                .lock()
                .unwrap()
                .push((self.id, settlement.clone()));
            Ok(())
        }

        async fn on_retry(&self, _attempt: u32, _delay: Duration) -> anyhow::Result<()> {
            self.retries.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Dead letter sink keeping every letter it receives.
    #[derive(Debug, Default)]
    struct CollectingSink {
        letters: Mutex<Vec<DeadLetter>>,
    }

    #[async_trait::async_trait]
    impl DeadLetterSink for CollectingSink {
        async fn dead_letter(&self, letter: DeadLetter) -> anyhow::Result<()> {
            self.letters.lock().unwrap().push(letter);
            Ok(())
        }
    }

    async fn wait_until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition was not met in time");
    }

    #[test]
    fn test_event_submission() {
        let event_message = EventMessage::new("test", "1234").unwrap();

        let sub_a = EventSubmission::from(&event_message);
        let sub_b = EventSubmission::from(event_message);
        assert_eq!(sub_a, sub_b);

        let sub_c = [("test", "1234"), ("foo", "bar")]
            .iter()
            .map(|(topic, content)| EventMessage::new(*topic, *content).unwrap())
            .collect::<EventSubmission>();
        if let EventSubmission::Batch(val) = sub_c {
            assert_eq!(val.len(), 2);
            assert_eq!(val[0], EventMessage::new("test", "1234").unwrap());
        }
    }

    #[test]
    fn test_topic_segment_parse() {
        let literal = TopicSegment::parse("test").unwrap();
        let wildcard = TopicSegment::parse("*").unwrap();
        let simple_selection = TopicSegment::parse("[five,four,six]").unwrap();
        let unsorted_and_duplicates_selection = TopicSegment::parse("[2, 1, 3, 2]").unwrap();
        let selection_with_wildcard = TopicSegment::parse("[one, two, *]").unwrap();
        let single_item_selection = TopicSegment::parse("[one]").unwrap();
        assert_eq!(TopicSegment::Literal("test".to_string()), literal);
        assert_eq!(TopicSegment::Wildcard, wildcard);
        assert_eq!(
            TopicSegment::Selection(vec![
                "five".to_string(),
                "four".to_string(),
                "six".to_string()
            ]),
            simple_selection
        );
        assert_eq!(
            TopicSegment::Selection(vec!["1".to_string(), "2".to_string(), "3".to_string(),]),
            unsorted_and_duplicates_selection
        );
        assert_eq!(TopicSegment::Wildcard, selection_with_wildcard);
        assert_eq!(
            TopicSegment::Literal("one".to_string()),
            single_item_selection
        );
        assert_eq!(
            TopicSegment::Literal("one".to_string()),
            TopicSegment::parse("[one,one]").unwrap()
        );
    }

    #[test]
    fn test_topic_segment_parse_rejects_malformed_segments() {
        use super::TopicError;

        assert_eq!(TopicSegment::parse(""), Err(TopicError::EmptySegment));
        assert_eq!(
            TopicSegment::parse("[a,]"),
            Err(TopicError::InvalidSelectionValue {
                segment: "[a,]".to_string(),
                value: String::new(),
            })
        );
        assert_eq!(
            TopicSegment::parse("[,]"),
            Err(TopicError::InvalidSelectionValue {
                segment: "[,]".to_string(),
                value: String::new(),
            })
        );
        assert_eq!(
            TopicSegment::parse("[]"),
            Err(TopicError::InvalidSelectionValue {
                segment: "[]".to_string(),
                value: String::new(),
            })
        );
        assert_eq!(
            TopicSegment::parse("[a[b]"),
            Err(TopicError::InvalidSelectionValue {
                segment: "[a[b]".to_string(),
                value: "a[b".to_string(),
            })
        );
        assert_eq!(
            TopicSegment::parse("[a*b,c]"),
            Err(TopicError::InvalidSelectionValue {
                segment: "[a*b,c]".to_string(),
                value: "a*b".to_string(),
            })
        );
        assert_eq!(
            TopicSegment::parse("a[b]"),
            Err(TopicError::InvalidSelection("a[b]".to_string()))
        );
        assert_eq!(
            TopicSegment::parse("[a"),
            Err(TopicError::InvalidSelection("[a".to_string()))
        );
        assert_eq!(
            TopicSegment::parse("a,b"),
            Err(TopicError::UnbracketedComma("a,b".to_string()))
        );
        assert_eq!(
            TopicSegment::parse("a*b"),
            Err(TopicError::EmbeddedWildcard("a*b".to_string()))
        );
    }

    #[test]
    fn test_topic_segment_matching() {
        let literal = TopicSegment::parse("test").unwrap();
        let wildcard = TopicSegment::parse("*").unwrap();
        let selection = TopicSegment::parse("[five,four,six]").unwrap();

        assert!(literal.matches(&TopicSegment::Literal("test".to_string())));
        assert!(!literal.matches(&TopicSegment::Literal("tset".to_string())));
        assert!(wildcard.matches(&TopicSegment::Literal("1234124¶áðfå".to_string())));
        assert!(wildcard.matches(&TopicSegment::Literal("fnord".to_string())));
        assert!(selection.matches(&TopicSegment::Literal("five".to_string())));
        assert!(selection.matches(&TopicSegment::Literal("four".to_string())));
        assert!(selection.matches(&TopicSegment::Literal("six".to_string())));
        assert!(!selection.matches(&TopicSegment::Literal("test".to_string())));
        assert!(!selection.matches(&TopicSegment::Literal("foo".to_string())));
    }

    #[test]
    fn test_event_topic() {
        let simple_topic = EventTopic::new("test.topic").as_routing_key().unwrap();
        let wildcard_topic = EventTopic::new("*").as_routing_key();
        let selection_topic = EventTopic::new("test.[one,two,three]")
            .as_subscription()
            .unwrap();
        let wildcard_tail_topic = EventTopic::new("test.*").as_subscription().unwrap();
        let wildcard_tail_topic_multiple = EventTopic::new("test.*.*").as_subscription().unwrap();
        let wildcard_tail_topic_selection = EventTopic::new("test.[one,two,*]")
            .as_subscription()
            .unwrap();
        let wildcard_tail_topic_selection_multiple = EventTopic::new("test.[one,two,*].*")
            .as_subscription()
            .unwrap();
        let topic_selection_wildcard_unsorted_duplicates =
            EventTopic::new("test.*.[5,2,4,3,5,2,1]")
                .as_subscription()
                .unwrap();

        assert_eq!(simple_topic.text(), "test.topic");
        assert!(wildcard_topic.is_err());
        assert_eq!(
            selection_topic.segments(),
            vec![
                TopicSegment::Literal("test".to_string()),
                TopicSegment::Selection(vec![
                    "one".to_string(),
                    "three".to_string(),
                    "two".to_string(),
                ])
            ]
        );
        assert!(wildcard_tail_topic.is_tail_matching);
        assert!(wildcard_tail_topic.match_topic(&simple_topic));
        assert_eq!(
            wildcard_tail_topic_multiple.segments(),
            vec![
                TopicSegment::Literal("test".to_string()),
                TopicSegment::Wildcard
            ]
        );
        assert!(wildcard_tail_topic_multiple.match_topic(&simple_topic));
        assert!(wildcard_tail_topic_selection.is_tail_matching);
        assert_eq!(
            wildcard_tail_topic_selection.segments(),
            vec![
                TopicSegment::Literal("test".to_string()),
                TopicSegment::Wildcard
            ]
        );
        assert!(wildcard_tail_topic_selection_multiple.is_tail_matching);
        assert!(
            wildcard_tail_topic_selection_multiple
                .match_topic(&EventTopic::new("test.foo.bar").as_routing_key().unwrap())
        );
        assert_eq!(
            wildcard_tail_topic_selection_multiple.segments(),
            vec![
                TopicSegment::Literal("test".to_string()),
                TopicSegment::Wildcard
            ]
        );
        assert_eq!(
            topic_selection_wildcard_unsorted_duplicates
                .segments()
                .len(),
            3
        );
        assert!(
            topic_selection_wildcard_unsorted_duplicates
                .match_topic(&EventTopic::new("test.foo.1").as_routing_key().unwrap())
        );
        assert!(
            topic_selection_wildcard_unsorted_duplicates
                .match_topic(&EventTopic::new("test.bar.5").as_routing_key().unwrap())
        );
        assert!(
            !topic_selection_wildcard_unsorted_duplicates
                .match_topic(&EventTopic::new("test.bar.fnord").as_routing_key().unwrap())
        );
        assert_eq!(
            topic_selection_wildcard_unsorted_duplicates.segments(),
            vec![
                TopicSegment::Literal("test".to_string()),
                TopicSegment::Wildcard,
                TopicSegment::Selection(vec![
                    "1".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string()
                ])
            ]
        );
    }

    #[tokio::test]
    async fn test_event_broker() {
        #[derive(Debug, Default, Clone)]
        struct TestConsumer {
            name: String,
            results: Arc<DashMap<String, Vec<EventMessage>>>,
            topic: String,
        }

        #[async_trait::async_trait]
        impl EventConsumer for TestConsumer {
            type Topic = ();

            fn subscriptions(&self) -> Subscriptions<()> {
                self.topic.as_str().into()
            }

            async fn handle_event(
                &self,
                _topic: &(),
                delivery: &Delivery,
            ) -> Result<(), HandlerError> {
                self.results
                    .entry(self.name.clone())
                    .or_default()
                    .push(delivery.message().as_ref().clone());
                Ok(())
            }
        }

        #[derive(Debug, Clone)]
        struct TestEmitter {
            sender: mpsc::Sender<EventMessage>,
        }

        #[async_trait::async_trait]
        impl EventEmitter for TestEmitter {
            fn get_sender(&self) -> &mpsc::Sender<EventMessage> {
                &self.sender
            }
        }

        let consumer_results = Arc::new(DashMap::new());

        let broker = EventBroker::new(10).run().unwrap();
        let consumer = TestConsumer {
            name: "consumer".to_string(),
            topic: String::from("test.topics.*"),
            results: consumer_results.clone(),
        };
        let consumer2 = TestConsumer {
            name: "consumer2".to_string(),
            topic: String::from("test.[one,two,three]"),
            results: consumer_results.clone(),
        };
        let consumer3 = TestConsumer {
            name: "consumer3".to_string(),
            topic: String::from("test.*.[5,2,4,3,5,2,1]"),
            results: consumer_results.clone(),
        };
        let consumer4 = TestConsumer {
            name: "consumer4".to_string(),
            topic: String::from("test.[should,fail]"),
            results: consumer_results.clone(),
        };
        broker.add_topic_consumer(consumer).unwrap();
        broker.add_topic_consumer(consumer2).unwrap();
        broker.add_topic_consumer(consumer3).unwrap();
        broker.add_topic_consumer(consumer4).unwrap();

        let events = [
            EventMessage::new("test.one", "consumer2 stuff test1").unwrap(),
            EventMessage::new("test.two", "consumer2 stuff test2").unwrap(),
            EventMessage::new("test.three", "consumer2 stuff test3").unwrap(),
            EventMessage::new("test.topics.foo", "consumer stuff test1").unwrap(),
            EventMessage::new("test.topics.1", "consumer and consumer3 stuff test").unwrap(),
            EventMessage::new("test.bar.5", "consumer3 stuff test").unwrap(),
            EventMessage::new("test.baz.foo", "non routeable stuff test1").unwrap(),
            EventMessage::new("test.four", "non routeable stuff test2").unwrap(),
        ];

        let emitter = TestEmitter {
            sender: broker.get_sender(),
        };

        emitter
            .submit_event(EventSubmission::from_iter(events[..4].iter().cloned()))
            .await
            .unwrap();
        emitter
            .submit_event(EventSubmission::Single(events.get(4).unwrap().clone()))
            .await
            .unwrap();
        emitter
            .submit_event(EventSubmission::Batch(events[5..7].to_vec()))
            .await
            .unwrap();
        emitter
            .submit_event(EventSubmission::from(events.last().unwrap().clone()))
            .await
            .unwrap();

        let _ = broker.stop().await;

        let consumer_messages = consumer_results.get("consumer").unwrap();
        let consumer2_messages = consumer_results.get("consumer2").unwrap();
        let consumer3_messages = consumer_results.get("consumer3").unwrap();

        assert!(consumer_messages.contains(events.get(3).unwrap()));
        assert!(consumer_messages.contains(events.get(4).unwrap()));
        assert!(consumer2_messages.contains(events.first().unwrap()));
        assert!(consumer2_messages.contains(events.get(1).unwrap()));
        assert!(consumer2_messages.contains(events.get(2).unwrap()));
        assert!(consumer3_messages.contains(events.get(4).unwrap()));
        assert!(consumer3_messages.contains(events.get(5).unwrap()));

        assert!(
            consumer_results
                .iter()
                .flat_map(|entry| entry.value().clone())
                .all(|v| v != *events.last().unwrap() && v != *events.get(6).unwrap())
        );
    }

    #[test]
    fn test_run_outside_runtime_fails() {
        assert!(EventBroker::new(1).run().is_err());
    }

    #[test]
    fn test_remove_topic_consumer() {
        #[derive(Debug)]
        struct NoopConsumer;

        #[async_trait::async_trait]
        impl EventConsumer for NoopConsumer {
            type Topic = ();

            fn subscriptions(&self) -> Subscriptions<()> {
                "test.remove".into()
            }

            async fn handle_event(
                &self,
                _topic: &(),
                _delivery: &Delivery,
            ) -> Result<(), HandlerError> {
                Ok(())
            }
        }

        let broker = EventBroker::new(1);
        let handle = broker.add_topic_consumer(NoopConsumer).unwrap();
        let routing_key = EventTopic::new("test.remove").as_routing_key().unwrap();
        assert_eq!(broker.find_subscriptions(&routing_key).len(), 1);

        assert!(broker.remove_topic_consumer(handle.clone()));
        assert!(broker.find_subscriptions(&routing_key).is_empty());
        assert!(broker.subscriptions().is_empty());
        assert!(!broker.remove_topic_consumer(handle));
    }

    #[test]
    fn test_consumer_with_invalid_pattern_is_not_registered() {
        #[derive(Debug)]
        struct PartlyValid;

        #[async_trait::async_trait]
        impl EventConsumer for PartlyValid {
            type Topic = u8;

            fn subscriptions(&self) -> Subscriptions<u8> {
                Subscriptions::new().on(1, "valid.*").on(2, "not valid")
            }

            async fn handle_event(
                &self,
                _topic: &u8,
                _delivery: &Delivery,
            ) -> Result<(), HandlerError> {
                Ok(())
            }
        }

        let broker = EventBroker::new(1);
        assert!(broker.add_topic_consumer(PartlyValid).is_err());
        assert!(broker.subscriptions().is_empty());
    }

    #[tokio::test]
    async fn test_stop_waits_for_in_flight_handlers() {
        #[derive(Debug)]
        struct SlowConsumer {
            results: Arc<DashMap<String, Vec<EventMessage>>>,
        }

        #[async_trait::async_trait]
        impl EventConsumer for SlowConsumer {
            type Topic = ();

            fn subscriptions(&self) -> Subscriptions<()> {
                "slow.*".into()
            }

            async fn handle_event(
                &self,
                _topic: &(),
                delivery: &Delivery,
            ) -> Result<(), HandlerError> {
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.results
                    .entry("slow".to_string())
                    .or_default()
                    .push(delivery.message().as_ref().clone());
                Ok(())
            }
        }

        #[derive(Debug)]
        struct TestEmitter {
            sender: mpsc::Sender<EventMessage>,
        }

        #[async_trait::async_trait]
        impl EventEmitter for TestEmitter {
            fn get_sender(&self) -> &mpsc::Sender<EventMessage> {
                &self.sender
            }
        }

        let results = Arc::new(DashMap::new());
        let broker = EventBroker::new(10);
        broker
            .add_topic_consumer(SlowConsumer {
                results: results.clone(),
            })
            .unwrap();
        let broker = broker.run().unwrap();
        let emitter = TestEmitter {
            sender: broker.get_sender(),
        };

        let events = (0..3)
            .map(|i| EventMessage::new(format!("slow.msg{i}"), "payload").unwrap())
            .collect::<Vec<_>>();
        emitter
            .submit_event(events.iter().cloned().collect())
            .await
            .unwrap();
        // Let the event loop pick up the messages so their handlers are in flight when we stop.
        tokio::task::yield_now().await;

        let _ = broker.stop().await;

        assert_eq!(results.get("slow").map(|v| v.len()), Some(events.len()));
    }

    #[tokio::test]
    async fn test_multi_topic_consumer_handles_topics_individually() {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum ShopTopic {
            Orders,
            EuPayments,
        }

        #[derive(Debug, Default)]
        struct Shop {
            received: Arc<Mutex<Vec<(ShopTopic, String)>>>,
        }

        #[async_trait::async_trait]
        impl EventConsumer for Shop {
            type Topic = ShopTopic;

            fn subscriptions(&self) -> Subscriptions<ShopTopic> {
                Subscriptions::new()
                    .on(ShopTopic::Orders, "orders.*")
                    .on(ShopTopic::EuPayments, "[orders,payments].eu.*")
            }

            async fn handle_event(
                &self,
                topic: &ShopTopic,
                delivery: &Delivery,
            ) -> Result<(), HandlerError> {
                let routing_key = delivery.message().topic().text().to_string();
                self.received.lock().unwrap().push((*topic, routing_key));
                Ok(())
            }
        }

        let shop = Shop::default();
        let received = shop.received.clone();

        // Consumers can be called without a broker.
        let delivery = Delivery::new(
            EventMessage::new("orders.test", "").unwrap(),
            EventTopic::new("orders.*").as_subscription().unwrap(),
        );
        shop.handle_event(&ShopTopic::Orders, &delivery)
            .await
            .unwrap();
        assert_eq!(received.lock().unwrap().len(), 1);
        received.lock().unwrap().clear();

        let broker = EventBroker::new(8).run().unwrap();
        let handle = broker.add_topic_consumer(shop).unwrap();
        let routed = broker.find_subscriptions(
            &EventTopic::new("orders.eu.created")
                .as_routing_key()
                .unwrap(),
        );
        assert_eq!(routed.len(), 2);
        assert!(routed.iter().all(|info| info.consumer().ends_with("Shop")));

        for topic in [
            "orders.us.created",
            "orders.eu.created",
            "payments.eu.settled",
            "payments.us.settled",
        ] {
            broker
                .get_sender()
                .send(EventMessage::new(topic, "").unwrap())
                .await
                .unwrap();
        }
        let broker = broker.stop().await;

        let mut received = received.lock().unwrap().clone();
        received.sort();
        assert_eq!(
            received,
            [
                (ShopTopic::Orders, "orders.eu.created".to_string()),
                (ShopTopic::Orders, "orders.us.created".to_string()),
                (ShopTopic::EuPayments, "orders.eu.created".to_string()),
                (ShopTopic::EuPayments, "payments.eu.settled".to_string()),
            ]
        );

        assert!(broker.remove_topic_consumer(handle));
        assert!(broker.subscriptions().is_empty());
    }

    #[tokio::test]
    async fn test_retries_until_handled_and_settles_once() {
        let settlements = Settlements::default();
        let retries = Arc::new(AtomicUsize::new(0));
        let (consumer, calls) = ScriptedConsumer::new(
            Subscription::new("jobs.*")
                .with_retry_policy(RetryPolicy::fixed(3, Duration::from_millis(1))),
            Script::FailAttempts(2),
        );
        let broker = EventBroker::new(4).run().unwrap();
        broker.add_topic_consumer(consumer).unwrap();

        let message = EventMessage::new("jobs.render", "job")
            .unwrap()
            .with_transport(RecordingHandle::new("job", &settlements, &retries));
        broker.get_sender().send(message).await.unwrap();
        let _ = broker.stop().await;

        let attempts = calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| (call.attempt, call.transport))
            .collect::<Vec<_>>();
        assert_eq!(
            attempts,
            [(1, Some("job")), (2, Some("job")), (3, Some("job"))]
        );
        assert_eq!(retries.load(Ordering::SeqCst), 2);
        let settlements = settlements.lock().unwrap();
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].1.all_handled());
    }

    #[tokio::test]
    async fn test_headers_reach_every_consumer_unchanged() {
        let (first, first_calls) = ScriptedConsumer::new("orders.*", Script::Succeed);
        let (second, second_calls) = ScriptedConsumer::new("orders.created", Script::Succeed);
        let broker = EventBroker::new(4).run().unwrap();
        broker.add_topic_consumer(first).unwrap();
        broker.add_topic_consumer(second).unwrap();

        let message = EventMessage::new("orders.created", "A-1")
            .unwrap()
            .with_header("correlation-id", "c-42")
            .with_headers([("traceparent", "00-abc-def-01")]);
        broker.get_sender().send(message).await.unwrap();
        let _ = broker.stop().await;

        for calls in [first_calls, second_calls] {
            let calls = calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0].headers.get("correlation-id").map(String::as_str),
                Some("c-42")
            );
            assert_eq!(
                calls[0].headers.get("traceparent").map(String::as_str),
                Some("00-abc-def-01")
            );
        }
    }

    #[test]
    fn test_clones_and_comparisons_include_headers_but_not_the_transport() {
        let settlements = Settlements::default();
        let retries = Arc::new(AtomicUsize::new(0));
        let original = EventMessage::new("jobs.render", "job")
            .unwrap()
            .with_header("event-id", "e-1")
            .with_transport(RecordingHandle::new("job", &settlements, &retries));

        let copy = original.clone();
        assert_eq!(copy, original);
        assert_eq!(copy.header("event-id"), Some("e-1"));
        assert!(copy.transport_handle().is_none());
        assert_ne!(copy.clone().with_header("event-id", "e-2"), original);
    }

    #[tokio::test]
    async fn test_settlement_reports_every_subscription() {
        #[derive(Debug)]
        struct FailingSink;

        #[async_trait::async_trait]
        impl DeadLetterSink for FailingSink {
            async fn dead_letter(&self, _letter: DeadLetter) -> anyhow::Result<()> {
                anyhow::bail!("dead letter queue unavailable")
            }
        }

        let settlements = Settlements::default();
        let retries = Arc::new(AtomicUsize::new(0));
        let sink = Arc::new(CollectingSink::default());
        let broker = EventBroker::new(4)
            .with_dead_letter_sink(sink.clone())
            .run()
            .unwrap();
        let consumers = [
            ScriptedConsumer::new("orders.*", Script::Succeed),
            ScriptedConsumer::new("orders.eu.*", Script::FailPermanently),
            ScriptedConsumer::new(
                Subscription::new("orders.[eu,us].created")
                    .with_dead_letter_sink(Arc::new(FailingSink)),
                Script::FailPermanently,
            ),
        ];
        for (consumer, _) in consumers {
            broker.add_topic_consumer(consumer).unwrap();
        }

        let sender = broker.get_sender();
        let routed = EventMessage::new("orders.eu.created", "{}")
            .unwrap()
            .with_transport(RecordingHandle::new("routed", &settlements, &retries));
        let unrouted = EventMessage::new("invoices.created", "{}")
            .unwrap()
            .with_transport(RecordingHandle::new("unrouted", &settlements, &retries));
        sender.send(routed).await.unwrap();
        sender.send(unrouted).await.unwrap();
        let _ = broker.stop().await;

        let mut settlements = settlements.lock().unwrap().clone();
        settlements.sort_by_key(|(id, _)| *id);
        let [("routed", routed), ("unrouted", unrouted)] = settlements.as_slice() else {
            panic!("expected one settlement per message, got {settlements:?}");
        };
        let mut outcomes = routed
            .outcomes()
            .iter()
            .map(|outcome| (outcome.subscription().pattern().text(), outcome.outcome()))
            .collect::<Vec<_>>();
        outcomes.sort_by_key(|(pattern, _)| *pattern);
        assert_eq!(
            outcomes,
            [
                ("orders.*", DeliveryOutcome::Handled),
                ("orders.[eu,us].created", DeliveryOutcome::Failed),
                ("orders.eu.*", DeliveryOutcome::DeadLettered),
            ]
        );
        assert!(!routed.all_handled() && !routed.any(DeliveryOutcome::Aborted));
        assert!(unrouted.is_unrouted() && !unrouted.all_handled());
        assert_eq!(retries.load(Ordering::SeqCst), 0);

        let letters = sink.letters.lock().unwrap();
        let [letter] = letters.as_slice() else {
            panic!("expected one dead letter, got {letters:?}");
        };
        assert_eq!(letter.subscription().pattern().text(), "orders.eu.*");
        assert_eq!(letter.attempts(), 1);
        assert_eq!(
            letter
                .message()
                .transport::<RecordingHandle>()
                .map(|handle| handle.id),
            Some("routed")
        );
        assert!(
            matches!(letter.reason(), DeadLetterReason::HandlerFailed(error) if error.is_permanent())
        );
    }

    #[tokio::test]
    async fn test_failed_deliveries_are_dead_lettered() {
        let sink = Arc::new(CollectingSink::default());
        let broker = EventBroker::new(4)
            .with_retry_policy(RetryPolicy::linear(
                2,
                Duration::from_millis(1),
                Duration::from_millis(1),
            ))
            .with_dead_letter_sink(sink.clone());
        let consumers = [
            ScriptedConsumer::new("flaky.*", Script::FailAttempts(u32::MAX)),
            ScriptedConsumer::new("broken.*", Script::FailPermanently),
            ScriptedConsumer::new("buggy.*", Script::Panic),
            ScriptedConsumer::new(
                Subscription::new("stubborn.*")
                    .with_retry_policy(RetryPolicy::exponential(4, Duration::from_millis(1))),
                Script::FailAttempts(u32::MAX),
            ),
        ];
        for (consumer, _) in consumers {
            broker.add_topic_consumer(consumer).unwrap();
        }
        let broker = broker.run().unwrap();

        for topic in ["flaky.job", "broken.job", "buggy.job", "stubborn.job"] {
            broker
                .get_sender()
                .send(EventMessage::new(topic, "job").unwrap())
                .await
                .unwrap();
        }
        let _ = broker.stop().await;

        let mut letters = sink
            .letters
            .lock()
            .unwrap()
            .iter()
            .map(|letter| {
                let DeadLetterReason::HandlerFailed(error) = letter.reason() else {
                    panic!("unexpected dead letter reason: {}", letter.reason());
                };
                (
                    letter.subscription().pattern().text().to_string(),
                    letter.attempts(),
                    error.is_permanent(),
                    error.to_string(),
                )
            })
            .collect::<Vec<_>>();
        letters.sort();
        assert_eq!(
            letters,
            [
                (
                    "broken.*".to_string(),
                    1,
                    true,
                    "malformed payload".to_string()
                ),
                (
                    "buggy.*".to_string(),
                    1,
                    true,
                    "event handler panicked: handler bug".to_string()
                ),
                (
                    "flaky.*".to_string(),
                    3,
                    false,
                    "attempt 3 failed".to_string()
                ),
                (
                    "stubborn.*".to_string(),
                    5,
                    false,
                    "attempt 5 failed".to_string()
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_sequential_subscription_retries_in_order() {
        let (consumer, calls) = ScriptedConsumer::new(
            Subscription::new("sequence.*")
                .with_concurrency(1)
                .with_retry_policy(RetryPolicy::fixed(1, Duration::from_millis(2))),
            Script::FailAttempts(1),
        );
        let broker = EventBroker::new(8);
        broker.add_topic_consumer(consumer).unwrap();
        let broker = broker.run().unwrap();

        for step in 0..4 {
            broker
                .get_sender()
                .send(EventMessage::new("sequence.step", step.to_string()).unwrap())
                .await
                .unwrap();
        }
        let _ = broker.stop().await;

        let calls = calls
            .lock()
            .unwrap()
            .iter()
            .map(|call| (call.content.clone(), call.attempt))
            .collect::<Vec<_>>();
        let expected = (0..4)
            .flat_map(|step| [(step.to_string(), 1), (step.to_string(), 2)])
            .collect::<Vec<_>>();
        assert_eq!(calls, expected);
    }

    #[tokio::test]
    async fn test_overflowing_inbox_is_dead_lettered() {
        #[derive(Debug)]
        struct GatedConsumer {
            gate: Arc<Semaphore>,
            handled: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl EventConsumer for GatedConsumer {
            type Topic = ();

            fn subscriptions(&self) -> Subscriptions<()> {
                Subscription::new("gated.*")
                    .with_inbox_capacity(1)
                    .with_concurrency(1)
                    .with_overflow(Overflow::DeadLetter)
                    .into()
            }

            async fn handle_event(
                &self,
                _topic: &(),
                _delivery: &Delivery,
            ) -> Result<(), HandlerError> {
                self.gate.acquire().await?.forget();
                self.handled.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let gate = Arc::new(Semaphore::new(0));
        let handled = Arc::new(AtomicUsize::new(0));
        let sink = Arc::new(CollectingSink::default());
        let broker = EventBroker::new(8)
            .with_dead_letter_sink(sink.clone())
            .run()
            .unwrap();
        broker
            .add_topic_consumer(GatedConsumer {
                gate: gate.clone(),
                handled: handled.clone(),
            })
            .unwrap();

        for job in 0..4 {
            broker
                .get_sender()
                .send(EventMessage::new("gated.job", job.to_string()).unwrap())
                .await
                .unwrap();
        }
        // While the handler is blocked, the subscription holds one message in flight and one in its
        // inbox, so at least two of them overflow.
        wait_until(|| sink.letters.lock().unwrap().len() >= 2).await;
        gate.add_permits(4);
        let _ = broker.stop().await;

        let letters = sink.letters.lock().unwrap();
        assert_eq!(handled.load(Ordering::SeqCst) + letters.len(), 4);
        assert!(letters.iter().all(|letter| {
            matches!(letter.reason(), DeadLetterReason::InboxFull) && letter.attempts() == 0
        }));
    }

    #[tokio::test]
    async fn test_subscriptions_survive_restarts_and_change_while_running() {
        let (first, first_calls) = ScriptedConsumer::new("lifecycle.*", Script::Succeed);
        let (second, second_calls) = ScriptedConsumer::new("lifecycle.*", Script::Succeed);
        let contents = |calls: &Calls| {
            calls
                .lock()
                .unwrap()
                .iter()
                .map(|call| call.content.clone())
                .collect::<Vec<_>>()
        };

        let broker = EventBroker::new(4);
        let first_handle = broker.add_topic_consumer(first).unwrap();
        let broker = broker.run().unwrap();
        broker
            .get_sender()
            .send(EventMessage::new("lifecycle.event", "1").unwrap())
            .await
            .unwrap();
        let broker = broker.stop().await.run().unwrap();

        broker
            .get_sender()
            .send(EventMessage::new("lifecycle.event", "2").unwrap())
            .await
            .unwrap();
        wait_until(|| first_calls.lock().unwrap().len() == 2).await;
        assert!(broker.remove_topic_consumer(first_handle));
        broker.add_topic_consumer(second).unwrap();
        broker
            .get_sender()
            .send(EventMessage::new("lifecycle.event", "3").unwrap())
            .await
            .unwrap();
        let _ = broker.stop().await;

        assert_eq!(contents(&first_calls), ["1", "2"]);
        assert_eq!(contents(&second_calls), ["3"]);
    }
}
