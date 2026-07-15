use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::sync::mpsc;
use topmesys::{
    EventBroker, EventConsumer, EventEmitter, EventMessage, EventSubmission, EventTopic,
};

#[derive(Debug)]
struct CountingConsumer {
    topic: String,
    counter: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl EventConsumer for CountingConsumer {
    fn consumes(&self) -> &String {
        &self.topic
    }

    async fn handle_event(&self, _event: Arc<EventMessage>) -> anyhow::Result<()> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug)]
struct BenchEmitter {
    sender: mpsc::Sender<EventMessage>,
}

#[async_trait::async_trait]
impl EventEmitter for BenchEmitter {
    fn get_sender(&self) -> &mpsc::Sender<EventMessage> {
        &self.sender
    }
}

fn bench_topic_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic_parsing");
    group.bench_function("routing_key", |b| {
        b.iter(|| {
            EventTopic::new(black_box("orders.eu.payment.settled"))
                .as_routing_key()
                .unwrap()
        })
    });
    group.bench_function("pattern_literal", |b| {
        b.iter(|| {
            EventTopic::new(black_box("orders.eu.payment.settled"))
                .as_subscription()
                .unwrap()
        })
    });
    group.bench_function("pattern_wildcard_tail", |b| {
        b.iter(|| {
            EventTopic::new(black_box("orders.eu.*"))
                .as_subscription()
                .unwrap()
        })
    });
    group.bench_function("pattern_selection", |b| {
        b.iter(|| {
            EventTopic::new(black_box("orders.[eu,us,apac].payment.*"))
                .as_subscription()
                .unwrap()
        })
    });
    group.finish();
}

fn bench_find_consumer(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_consumer");
    for subscriptions in [10usize, 100, 1000] {
        let broker = EventBroker::new(64);
        let counter = Arc::new(AtomicUsize::new(0));
        for i in 0..subscriptions {
            broker
                .add_topic_consumer(CountingConsumer {
                    topic: format!("bench.sub{i}.*"),
                    counter: counter.clone(),
                })
                .unwrap();
        }
        let topic = EventTopic::new(format!("bench.sub{}.deep.nested", subscriptions / 2))
            .as_routing_key()
            .unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(subscriptions),
            &subscriptions,
            |b, _| b.iter(|| broker.find_consumer(black_box(&topic))),
        );
    }
    group.finish();
}

fn bench_dispatch(c: &mut Criterion) {
    const BATCH: usize = 256;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let broker = EventBroker::new(BATCH);
    broker
        .add_topic_consumer(CountingConsumer {
            topic: "bench.dispatch.*".to_string(),
            counter: counter.clone(),
        })
        .unwrap();
    let broker = rt.block_on(async { broker.run() }).unwrap();
    let emitter = BenchEmitter {
        sender: broker.get_sender(),
    };
    let messages = (0..BATCH)
        .map(|i| EventMessage::new(format!("bench.dispatch.msg{i}"), "payload").unwrap())
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("dispatch");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("batch_256", |b| {
        b.to_async(&rt).iter(|| async {
            let handled = counter.load(Ordering::Relaxed);
            emitter
                .submit_event(messages.iter().cloned().collect::<EventSubmission>())
                .await
                .unwrap();
            while counter.load(Ordering::Relaxed) < handled + BATCH {
                tokio::task::yield_now().await;
            }
        })
    });
    group.finish();

    rt.block_on(async { broker.stop().await });
}

criterion_group!(
    benches,
    bench_topic_parsing,
    bench_find_consumer,
    bench_dispatch
);
criterion_main!(benches);
