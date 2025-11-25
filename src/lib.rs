use std::{borrow::Borrow, fmt::Display, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{StreamExt, future::select};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use type_states::{Init, Pattern, RoutingKey, Running, StateMarker, Stopped};

pub type EventSubscriptions = Arc<DashMap<EventTopic<Pattern>, Vec<Arc<dyn EventConsumer>>>>;

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
    }

    pub trait StateMarker {}
    impl StateMarker for () {}
    impl StateMarker for Init {}
    impl StateMarker for RoutingKey {}
    impl StateMarker for Pattern {}

    impl StateMarker for Stopped {}
    impl StateMarker for Running {}
}

/// A type representing a single topic segment. Topic segments are used to match against other topic segments. Literal segments
/// matched against other literal segments must be equal. Selection segments are matched against literal segments by checking if the literal segment
/// value is contained within the selection segment. Wildcard segments match against any segment. Selection segments are defined by square brackets and comma separated values.
/// When parsing selection segments, and inside the selection a wildcard segment is found, the entire selection will be parsed as wildcard. If a selection segment contains only a single
/// value, the segment will be parsed as literal. Multiple values will be sorted and deduplicated, so selection segment lookups should be fairly quick.
/// Wildcard segments are defined by a single asterisk. Literal segments are any other string.
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

    fn parse(text: &str) -> anyhow::Result<Self> {
        let literal_wildcard = "*".to_string();
        if text.is_empty() {
            anyhow::bail!("Found empty segment.");
        }
        if text == literal_wildcard {
            return Ok(Self::Wildcard);
        }
        if text.starts_with('[') && !text.ends_with(']')
            || text.ends_with(']') && !text.starts_with('[')
            || text.starts_with('[') && text.len() <= 2
        {
            anyhow::bail!("Invalid selection segment: {text}");
        }
        if !text.starts_with('[') && text.contains(',') {
            anyhow::bail!("Found `,` outside of selection segment: {text}");
        }
        if text.starts_with('[') && text.ends_with(']') {
            let mut bracketed = text[1..text.len() - 1]
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>();
            if bracketed.contains(&literal_wildcard) {
                Ok(Self::Wildcard)
            } else if bracketed.len() == 1 {
                Ok(Self::Literal(bracketed[0].clone()))
            } else {
                bracketed.sort();
                bracketed.dedup();
                Ok(Self::Selection(bracketed))
            }
        } else {
            Ok(Self::Literal(text.to_string()))
        }
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
    pub fn as_routing_key(self) -> anyhow::Result<EventTopic<RoutingKey>> {
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

    pub fn as_subscription(self) -> anyhow::Result<EventTopic<Pattern>> {
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

    fn parse_segments(&self) -> anyhow::Result<Vec<TopicSegment>> {
        let mut parsed_segments = Vec::new();
        for segment in self.raw.split('.') {
            parsed_segments.push(TopicSegment::parse(segment)?);
        }
        Ok(parsed_segments)
    }

    fn sanitize_topic(&self, extra_keys: &[char]) -> anyhow::Result<()> {
        if self.raw.is_empty() {
            anyhow::bail!("Topic cannot be empty.");
        }
        if !self
            .raw
            .chars()
            .all(|c| c.is_alphanumeric() || extra_keys.contains(&c))
        {
            anyhow::bail!(
                "Invalid character in topic: {}. Topics may only contain alphanumeric characters and {extra_keys:?}.",
                self.raw
            );
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

impl<B: Borrow<EventMessage>> From<B> for EventSubmission {
    fn from(value: B) -> Self {
        Self::Single(value.borrow().to_owned())
    }
}

impl FromIterator<EventMessage> for EventSubmission {
    fn from_iter<T: IntoIterator<Item = EventMessage>>(iter: T) -> Self {
        Self::Batch(Vec::from_iter(iter))
    }
}

/// Enables implementors to submit one or more [EventMessage]s to a [channel](Sender) connected
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

/// Implementors are registered with an [EventBroker] and receive [EventMessage]s based on [EventTopic]s.
#[async_trait::async_trait]
pub trait EventConsumer: std::fmt::Debug + Send + Sync {
    fn consumes(&self) -> &String;
    async fn handle_event(&self, event: Arc<EventMessage>) -> anyhow::Result<()>;
}

/// This is the central bus for all events in the application. It sets up a [channel](tokio::sync::mpsc) and dispatches
/// received [EventMessage]s to [EventConsumer]s subscribed to matching [EventTopic]s by calling the consumers [handle_event](EventConsumer::handle_event)
/// method. If an events topic can't be matched to any subscribed consumers, the message will be dropped silently. If the consumers event handling method returns
/// an error, the error is logged and the event discarded. Future development will expand this behavior with proper routing policies and error handling.
/// Messages are submitted to the channel using a [Sender<EventMessage>], produced by the [get_sender](EventBroker::get_sender) method.
/// While the sender can be used for submissions "as is", the [submit_event][EventEmitter::submit_event] from the [EventEmitter] trait provides a safe default
/// implementation using [channel permits](tokio::sync::mpsc::Permit), conveniently supporting both single and batched [EventSubmission]s.
#[derive(Debug)]
pub struct EventBroker<S: StateMarker> {
    runtime: S,
    subscriptions: EventSubscriptions,
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
        }
    }
}

impl<S> EventBroker<S>
where
    S: StateMarker + 'static,
{
    pub fn add_topic_consumer(&self, consumer: impl EventConsumer + 'static) -> anyhow::Result<()> {
        let subscription_pattern = EventTopic::new(consumer.consumes()).as_subscription()?;
        self.subscriptions
            .entry(subscription_pattern)
            .or_default()
            .push(Arc::new(consumer));
        Ok(())
    }

    pub fn get_subscriptions(&self) -> &EventSubscriptions {
        &self.subscriptions
    }

    pub fn find_consumer(&self, topic: &EventTopic<RoutingKey>) -> Vec<Arc<dyn EventConsumer>> {
        self.subscriptions
            .iter()
            .filter(|entry| entry.key().match_topic(topic))
            .flat_map(|entry| entry.value().clone())
            .collect()
    }

    #[tracing::instrument(skip(self))]
    async fn run_event_loop(
        &self,
        receiver: mpsc::Receiver<EventMessage>,
        mut stop_rx: watch::Receiver<bool>,
    ) {
        let stop_signal = tokio::signal::ctrl_c();
        let stop_call = stop_rx.changed();
        let stop = select(Box::pin(stop_signal), Box::pin(stop_call));
        let mut event_stream = ReceiverStream::new(receiver);
        tokio::select! {
            biased;
            _ = stop => {
                tracing::info!("Stopping event stream processing.");
                event_stream.close();
                event_stream.for_each(|event_msg| async move {Self::publish_event(self.find_consumer(event_msg.topic()), Arc::new(event_msg.clone())).await}).await;
            }
            _ = async {
                event_stream.by_ref().for_each_concurrent(0, |event_msg| async move {
                    tokio::spawn(Self::publish_event(self.find_consumer(event_msg.topic()), Arc::new(event_msg.clone())));
                }).await;
            } => {
                tracing::info!("Event loop processing ended. Shutting down broker. ");
            }
        }
    }

    #[tracing::instrument()]
    async fn publish_event(consumers: Vec<Arc<dyn EventConsumer>>, event_msg: Arc<EventMessage>) {
        for consumer in consumers {
            if let Err(e) = consumer.handle_event(event_msg.clone()).await {
                tracing::error!("{}", e.to_string())
            }
        }
    }
}

impl EventBroker<Stopped> {
    pub fn run(self) -> anyhow::Result<EventBroker<Running>> {
        let rt = tokio::runtime::Handle::current();
        let (message_tx, message_rx) = mpsc::channel::<EventMessage>(self.runtime.bufsize);
        let (stop_tx, stop_rx) = watch::channel(false);
        let broker = EventBroker {
            subscriptions: self.subscriptions.clone(),
            runtime: Running {
                handle: rt.spawn(async move { self.run_event_loop(message_rx, stop_rx).await }),
                message_tx,
                stop_tx,
            },
        };

        Ok(broker)
    }
}

impl EventBroker<Running> {
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
        EventBroker {
            runtime: Stopped {
                bufsize: self.runtime.message_tx.max_capacity(),
            },
            subscriptions: self.subscriptions.clone(),
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<EventMessage> {
        self.runtime.message_tx.clone()
    }
}

/// A type representing an event handled by the applications event bus, consisting
/// of a [Into] [String] subject and [Into] [Bytes] content.
/// #### Example
/// ```
/// use topmesys::{EventMessage, EventTopic};
///
/// let content = vec![123, 34, 116, 111, 34, 58, 34, 116, 104, 101, 109, 34, 125];
///
/// let first_msg = EventMessage::new("my-message", content).unwrap();
/// let second_msg = EventMessage::default()
///     .with_topic(EventTopic::new("my-message").as_routing_key().unwrap())
///     .with_content(r#"{"to":"them"}"#);
///
/// assert_eq!(first_msg.topic(), second_msg.topic());
/// assert_eq!(first_msg.content(), second_msg.content());
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventMessage {
    topic: EventTopic<RoutingKey>,
    content: Bytes,
}

impl EventMessage {
    pub fn new(topic_text: impl Into<String>, content: impl Into<Bytes>) -> anyhow::Result<Self> {
        Ok(Self {
            topic: EventTopic::new(topic_text.into()).as_routing_key()?,
            content: content.into(),
        })
    }

    pub fn topic(&self) -> &EventTopic<RoutingKey> {
        &self.topic
    }

    pub fn content(&self) -> &Bytes {
        &self.content
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
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::Arc};

    use dashmap::DashMap;
    use tokio::sync::mpsc;

    use super::{
        EventBroker, EventConsumer, EventEmitter, EventMessage, EventSubmission, EventTopic,
        TopicSegment,
    };

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
            fn consumes(&self) -> &String {
                &self.topic
            }

            async fn handle_event(&self, event: Arc<EventMessage>) -> anyhow::Result<()> {
                self.results
                    .entry(self.name.clone())
                    .or_default()
                    .push(event.deref().clone());
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

        println!("{consumer_results:#?}");
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
}
