use std::time::Duration;

use proxy_core::{
    ActiveConnectionCounter, AdmissionError, AdmissionLimiter, Shutdown, TimeoutError,
};
use tokio::sync::oneshot;

#[tokio::test]
async fn shutdown_cancellation_releases_admission_and_active_lifecycle() {
    let limiter = AdmissionLimiter::new(1);
    let counter = ActiveConnectionCounter::new();
    let shutdown = Shutdown::new();
    let (entered_tx, entered_rx) = oneshot::channel();

    let worker = tokio::spawn({
        let limiter = limiter.clone();
        let counter = counter.clone();
        let shutdown = shutdown.clone();

        async move {
            let _permit = limiter.try_acquire().expect("worker admitted");
            let _active = counter.track();

            entered_tx.send(()).expect("test waits for active worker");
            shutdown.cancelled().await;
        }
    });

    entered_rx.await.expect("worker entered active lifecycle");
    assert_eq!(limiter.in_flight(), 1);
    assert_eq!(counter.active(), 1);
    assert_eq!(
        limiter.try_acquire().expect_err("admission is saturated"),
        AdmissionError::Saturated { limit: 1 }
    );

    shutdown.shutdown();
    worker.await.expect("worker completed after cancellation");
    counter.wait_for_zero().await;

    assert_eq!(limiter.in_flight(), 0);
    assert_eq!(counter.active(), 0);

    let _permit = limiter
        .try_acquire()
        .expect("admission capacity is reusable after cancellation");
}

#[tokio::test(start_paused = true)]
async fn timeout_helper_composes_with_active_lifecycle_without_leaking() {
    let counter = ActiveConnectionCounter::new();
    let worker_counter = counter.clone();

    let result = proxy_core::with_timeout(Duration::from_secs(5), async move {
        let _active = worker_counter.track();
        tokio::time::sleep(Duration::from_secs(10)).await;
    })
    .await;

    assert_eq!(result, Err(TimeoutError::new(Duration::from_secs(5))));
    counter.wait_for_zero().await;
    assert_eq!(counter.active(), 0);
}
