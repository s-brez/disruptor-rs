use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering::{Acquire, Release}};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
use criterion::measurement::WallTime;

use crossbeam::channel::TrySendError::Full;
use crossbeam::channel::{bounded, TryRecvError::{Empty, Disconnected}};

use disruptor::{BusySpin, PhasedBackoff, Producer};

const DATA_STRUCTURE_SIZE: usize    = 256;
const BURST_SIZES:         [u64; 3] = [1, 10, 100];
const PAUSES_MS:           [u64; 3] = [0, 1, 10];
const CONSUMER_COUNTS:     [usize; 2] = [2, 3];

struct Event {
    data: i64,
}

fn pause(millis: u64) {
    if millis > 0 {
        thread::sleep(Duration::from_millis(millis));
    }
}

pub fn spmc_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spmc");

    for &burst_size in BURST_SIZES.iter() {
        group.throughput(Throughput::Elements(burst_size));

        base(&mut group, burst_size as i64);

        for &consumers in CONSUMER_COUNTS.iter() {
            for &pause_ms in PAUSES_MS.iter() {
                let inputs = (burst_size as i64, pause_ms);
                let param  = format!("consumers: {}, burst: {}, pause: {} ms", consumers, burst_size, pause_ms);

                crossbeam(&mut group, consumers, inputs, &param);
                disruptor(&mut group, consumers, inputs, &param);
                disruptor_phased_backoff(&mut group, consumers, inputs, &param);
            }
        }
    }

    group.finish();
}

fn base(group: &mut BenchmarkGroup<WallTime>, burst_size: i64) {
    let sink         = Arc::new(AtomicI64::new(0));
    let benchmark_id = BenchmarkId::new("base", burst_size);

    group.bench_with_input(benchmark_id, &burst_size, move |b, size| b.iter_custom(|iters| {
        let start = Instant::now();
        for _ in 0..iters {
            sink.store(0, Release);
            for _ in 0..*size {
                sink.fetch_add(1, Release);
            }
            // Wait until all increments have been observed.
            while sink.load(Acquire) != *size {}
        }
        start.elapsed()
    }));
}

fn crossbeam(group: &mut BenchmarkGroup<WallTime>, consumers: usize, inputs: (i64, u64), param: &str) {
    let sink     = Arc::new(AtomicI64::new(0));
    let (s, r)   = bounded::<Event>(DATA_STRUCTURE_SIZE);

    let mut consumer_threads = Vec::with_capacity(consumers);
    for _ in 0..consumers {
        let sink = Arc::clone(&sink);
        let r    = r.clone();
        consumer_threads.push(thread::spawn(move || {
            loop {
                match r.try_recv() {
                    Ok(event)         => {
                        black_box(event.data);
                        sink.fetch_add(1, Release);
                    },
                    Err(Empty)        => continue,
                    Err(Disconnected) => break,
                }
            }
        }));
    }

    let benchmark_id = BenchmarkId::new("Crossbeam", param);
    group.bench_with_input(benchmark_id, &inputs, move |b, (size, pause_ms)| b.iter_custom(|iters| {
        pause(*pause_ms);
        let expected_count = *size as i64;
        let start = Instant::now();

        for _ in 0..iters {
            sink.store(0, Release);
            for data in 0..*size {
                let mut event = Event { data: black_box(data) };
                loop {
                    match s.try_send(event) {
                        Err(Full(e)) => event = e,
                        _            => break,
                    }
                }
            }

            while sink.load(Acquire) != expected_count {}
        }

        start.elapsed()
    }));

    for th in consumer_threads {
        th.join().expect("Consumer thread panicked");
    }
}

fn disruptor(group: &mut BenchmarkGroup<WallTime>, consumers: usize, inputs: (i64, u64), param: &str) {
    let factory = || { Event { data: 0 } };

    let sink = Arc::new(AtomicI64::new(0));

    let mut producer = match consumers {
        2 => {
            let sink1 = Arc::clone(&sink);
            let sink2 = Arc::clone(&sink);
            disruptor::build_single_producer(DATA_STRUCTURE_SIZE, factory, BusySpin)
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink1.fetch_add(1, Release);
                })
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink2.fetch_add(1, Release);
                })
                .build()
        },
        3 => {
            let sink1 = Arc::clone(&sink);
            let sink2 = Arc::clone(&sink);
            let sink3 = Arc::clone(&sink);
            disruptor::build_single_producer(DATA_STRUCTURE_SIZE, factory, BusySpin)
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink1.fetch_add(1, Release);
                })
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink2.fetch_add(1, Release);
                })
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink3.fetch_add(1, Release);
                })
                .build()
        },
        _ => unreachable!("Only 2 or 3 consumers are supported"),
    };

    let expected_multiplier = consumers as i64;
    let benchmark_id        = BenchmarkId::new("Disruptor (busy-spin)", param);

    group.bench_with_input(benchmark_id, &inputs, move |b, (size, pause_ms)| b.iter_custom(|iters| {
        pause(*pause_ms);
        let start = Instant::now();

        for _ in 0..iters {
            sink.store(0, Release);

            producer.batch_publish(*size as usize, |iter| {
                for (i, e) in iter.enumerate() {
                    e.data = black_box(i as i64);
                }
            });

            let expected = *size * expected_multiplier;
            while sink.load(Acquire) != expected {}
        }

        start.elapsed()
    }));
}

fn disruptor_phased_backoff(group: &mut BenchmarkGroup<WallTime>, consumers: usize, inputs: (i64, u64), param: &str) {
    let factory = || { Event { data: 0 } };

    let sink = Arc::new(AtomicI64::new(0));

    let wait_strategy = PhasedBackoff::with_sleep(Duration::from_millis(1), Duration::from_millis(1));

    let mut producer = match consumers {
        2 => {
            let sink1 = Arc::clone(&sink);
            let sink2 = Arc::clone(&sink);
            disruptor::build_single_producer(DATA_STRUCTURE_SIZE, factory, wait_strategy.clone())
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink1.fetch_add(1, Release);
                })
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink2.fetch_add(1, Release);
                })
                .build()
        },
        3 => {
            let sink1 = Arc::clone(&sink);
            let sink2 = Arc::clone(&sink);
            let sink3 = Arc::clone(&sink);
            disruptor::build_single_producer(DATA_STRUCTURE_SIZE, factory, wait_strategy.clone())
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink1.fetch_add(1, Release);
                })
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink2.fetch_add(1, Release);
                })
                .handle_events_with(move |event: &Event, _seq, _eob| {
                    black_box(event.data);
                    sink3.fetch_add(1, Release);
                })
                .build()
        },
        _ => unreachable!("Only 2 or 3 consumers are supported"),
    };

    let expected_multiplier = consumers as i64;
    let benchmark_id        = BenchmarkId::new("Disruptor (phased backoff)", param);

    group.bench_with_input(benchmark_id, &inputs, move |b, (size, pause_ms)| b.iter_custom(|iters| {
        pause(*pause_ms);
        let start = Instant::now();

        for _ in 0..iters {
            sink.store(0, Release);

            producer.batch_publish(*size as usize, |iter| {
                for (i, e) in iter.enumerate() {
                    e.data = black_box(i as i64);
                }
            });

            let expected = *size * expected_multiplier;
            while sink.load(Acquire) != expected {}
        }

        start.elapsed()
    }));
}

criterion_group!(spmc, spmc_benchmark);
criterion_main!(spmc);
