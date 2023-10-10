use crate::shutdown::ShutdownNotifier;
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::info;

pub(crate) static RUNNING_TASKS: AtomicUsize = AtomicUsize::new(0);

const EXP_1: f64 = 0.9200; // 1/exp(5sec/1min)
const EXP_5: f64 = 0.9835; // 1/exp(5sec/5min)
const EXP_15: f64 = 0.9945; // 1/exp(5sec/15min)

lazy_static! {
    static ref LOAD_AVG_1MIN: Gauge =
        register_gauge!("executor_load_avg_1m", "One minute load average").unwrap();
    static ref LOAD_AVG_5MIN: Gauge =
        register_gauge!("executor_load_avg_5m", "Five minute load average").unwrap();
    static ref LOAD_AVG_15MIN: Gauge =
        register_gauge!("executor_load_avg_15m", "Fifteen minute load average").unwrap();
}

pub fn init_load_avg(shutdown_noti: &ShutdownNotifier) {
    let mut shutdown = shutdown_noti.subscribe_for_shutdown();
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    tokio::spawn(async move {
        info!("starting load average monitor");
        // As long as the shutdown notification has not been received
        while !shutdown.is_shutdown() {
            tokio::select! {
                _ = interval.tick() => {
                    // Calculation taken from https://github.com/torvalds/linux/blob/master/kernel/sched/loadavg.c
                    let num_tasks = RUNNING_TASKS.load(Ordering::Relaxed) as f64;

                    let current = LOAD_AVG_1MIN.get();
                    LOAD_AVG_1MIN.set(current * EXP_1 + num_tasks * (1. - EXP_1));

                    let current = LOAD_AVG_5MIN.get();
                    LOAD_AVG_5MIN.set(current * EXP_5 + num_tasks * (1. - EXP_5));

                    let current = LOAD_AVG_15MIN.get();
                    LOAD_AVG_15MIN.set(current * EXP_15 + num_tasks * (1. - EXP_15));
                },
                _ = shutdown.recv() => {
                    info!("stopping load average monitor");
                    return;
                }
            };
        }
    });
}
