//! A smart-building sensor hub.
//!
//! Sensors publish readings to `sensors.<floor>.<kind>` topics. Two consumers
//! subscribe with different pattern styles:
//! - an alerting service watching temperature readings on any floor
//!   (`sensors.*.temperature`, a mid-pattern wildcard)
//! - a logger recording everything from selected floors
//!   (`sensors.[floor1,floor2].*`, a selection segment with tail matching)
//!
//! Run with: `cargo run --example sensor_hub`

use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc;
use topmesys::{EventBroker, EventConsumer, EventEmitter, EventMessage, EventSubmission};

#[derive(Debug)]
struct TemperatureAlert {
    topic: String,
    threshold: f32,
}

#[async_trait::async_trait]
impl EventConsumer for TemperatureAlert {
    fn consumes(&self) -> &String {
        &self.topic
    }

    async fn handle_event(&self, event: Arc<EventMessage>) -> anyhow::Result<()> {
        let reading: f32 = std::str::from_utf8(event.content())?.parse()?;
        if reading > self.threshold {
            println!(
                "[alert]  {}: {reading}°C exceeds threshold of {}°C!",
                event.topic(),
                self.threshold
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
struct FloorLogger {
    topic: String,
}

#[async_trait::async_trait]
impl EventConsumer for FloorLogger {
    fn consumes(&self) -> &String {
        &self.topic
    }

    async fn handle_event(&self, event: Arc<EventMessage>) -> anyhow::Result<()> {
        println!(
            "[logger] {} = {}",
            event.topic(),
            std::str::from_utf8(event.content())?
        );
        Ok(())
    }
}

#[derive(Debug)]
struct SensorGateway {
    sender: mpsc::Sender<EventMessage>,
}

#[async_trait::async_trait]
impl EventEmitter for SensorGateway {
    fn get_sender(&self) -> &mpsc::Sender<EventMessage> {
        &self.sender
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let broker = EventBroker::new(64);
    broker.add_topic_consumer(TemperatureAlert {
        topic: "sensors.*.temperature".to_string(),
        threshold: 30.0,
    })?;
    broker.add_topic_consumer(FloorLogger {
        topic: "sensors.[floor1,floor2].*".to_string(),
    })?;

    let broker = broker.run()?;
    let gateway = SensorGateway {
        sender: broker.get_sender(),
    };

    // A batch of readings arriving from the field.
    let readings = [
        ("sensors.floor1.temperature", "21.5"),
        ("sensors.floor2.temperature", "34.2"),
        ("sensors.floor3.temperature", "19.8"),
        ("sensors.floor1.humidity", "40"),
        ("sensors.floor2.co2", "600"),
        ("sensors.basement.humidity", "80"),
    ]
    .into_iter()
    .map(|(topic, value)| EventMessage::new(topic, value))
    .collect::<anyhow::Result<EventSubmission>>()?;

    gateway.submit_event(readings).await?;

    // Give in-flight handlers a moment before shutting down; handlers picked up
    // before the stop signal are processed on detached tasks.
    tokio::time::sleep(Duration::from_millis(100)).await;
    broker.stop().await;

    Ok(())
}
