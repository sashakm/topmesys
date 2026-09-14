use std::{fmt::Display, sync::Arc};

use crate::{
    EventMessage, EventTopic, SubscriptionInfo, TransportHandle,
    transport::{DeliveryOutcome, Settler},
    type_states::Pattern,
};

/// A message handed to an [EventConsumer](crate::EventConsumer) on one of its subscriptions.
#[derive(Debug)]
pub struct Delivery {
    pub(crate) message: Arc<EventMessage>,
    pub(crate) subscription: Arc<SubscriptionInfo>,
    pub(crate) attempt: u32,
    pub(crate) settler: Option<Arc<Settler>>,
}

impl Delivery {
    /// Creates a delivery on the given pattern outside of a broker, e.g. to test an
    /// [EventConsumer](crate::EventConsumer). Its [consumer](SubscriptionInfo::consumer) is empty and
    /// it never settles the message.
    pub fn new(message: impl Into<Arc<EventMessage>>, pattern: EventTopic<Pattern>) -> Self {
        Self {
            message: message.into(),
            subscription: Arc::new(SubscriptionInfo {
                pattern,
                consumer: "",
            }),
            attempt: 1,
            settler: None,
        }
    }

    /// Self-consuming attempt setter
    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn message(&self) -> &Arc<EventMessage> {
        &self.message
    }

    pub fn subscription(&self) -> &SubscriptionInfo {
        &self.subscription
    }

    /// The current attempt at handling the message, starting at `1`.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The message's transport handle, if it carries one of type `T`.
    pub fn transport<T: TransportHandle>(&self) -> Option<&T> {
        self.message.transport()
    }

    /// Records the delivery's outcome, settling the message if no other delivery of it is open.
    pub(crate) async fn finish(mut self, outcome: DeliveryOutcome) {
        let Some(settler) = self.settler.take() else {
            return;
        };
        settler.record(&self.subscription, outcome);
        if let Some(settler) = Arc::into_inner(settler) {
            settler.settle().await;
        }
    }
}

impl Drop for Delivery {
    /// A delivery still holding its settler was never finished, e.g. because its worker was aborted.
    /// It is recorded as aborted, so the message still settles and its transport can redeliver it.
    fn drop(&mut self) {
        let Some(settler) = self.settler.take() else {
            return;
        };
        settler.record(&self.subscription, DeliveryOutcome::Aborted);
        if let Some(settler) = Arc::into_inner(settler) {
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    runtime.spawn(settler.settle());
                }
                Err(_) => tracing::error!(
                    "Unable to settle aborted event on `{}`: no tokio runtime available",
                    self.message.topic()
                ),
            }
        }
    }
}

/// The error returned by [EventConsumer::handle_event](crate::EventConsumer::handle_event). Any
/// error converts into a transient `HandlerError` with `?`, which is retried according to the
/// subscription's [RetryPolicy](crate::RetryPolicy). Errors retrying won't fix should be returned as
/// [permanent](HandlerError::permanent), which dead-letters the message right away. Like
/// [anyhow::Error], `HandlerError` doesn't implement [std::error::Error] itself, as that would rule
/// out the conversion.
#[derive(Debug)]
pub struct HandlerError {
    error: anyhow::Error,
    permanent: bool,
}

impl HandlerError {
    /// An error worth retrying, e.g. a timeout. Same as converting the error with `?`.
    pub fn transient(error: impl Into<anyhow::Error>) -> Self {
        Self {
            error: error.into(),
            permanent: false,
        }
    }

    /// An error retrying won't fix, e.g. a malformed payload.
    pub fn permanent(error: impl Into<anyhow::Error>) -> Self {
        Self {
            error: error.into(),
            permanent: true,
        }
    }

    pub fn is_permanent(&self) -> bool {
        self.permanent
    }

    pub fn error(&self) -> &anyhow::Error {
        &self.error
    }

    pub fn into_error(self) -> anyhow::Error {
        self.error
    }
}

impl<E> From<E> for HandlerError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self::transient(error)
    }
}

impl Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.error, f)
    }
}
