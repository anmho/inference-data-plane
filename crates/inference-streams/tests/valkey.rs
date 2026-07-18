use std::time::Duration;

use inference_streams::{
    QuotaLimits, SCHEMA_VERSION, StreamsClient, StreamsConfig,
    wire::{
        AttemptStarted, GenerationCompleted, GenerationJob, GenerationResult, TokenChunk,
        generation_result::Event,
    },
};

async fn test_client(database: u8) -> Option<StreamsClient> {
    let base_url = std::env::var("VALKEY_TEST_URL").ok()?;
    let url = format!("{}/{database}", base_url.trim_end_matches('/'));
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    Some(
        StreamsClient::connect(
            &url,
            StreamsConfig {
                active_ttl: Duration::from_secs(10),
                result_retention: Duration::from_secs(1),
                lease_duration: Duration::from_secs(1),
            },
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn jobs_ack_while_results_fan_out_and_replay() {
    let Some(streams) = test_client(1).await else {
        return;
    };
    let job = GenerationJob {
        schema_version: SCHEMA_VERSION,
        request_id: "request-1".into(),
        model_id: "model".into(),
        request_json: b"{}".to_vec(),
        reservation_id: "reservation".into(),
        ..Default::default()
    };
    streams.publish_generation(&job).await.unwrap();
    let queued = streams.status(&job.request_id).await.unwrap().unwrap();
    assert_eq!(queued.state, "queued");
    assert_eq!(queued.model_id, "model");
    assert_eq!(queued.active_attempt_id, None);
    assert_eq!(queued.terminal_event_id, None);
    let mut jobs = streams.consumer("engine-1").await.unwrap();
    let consumed = jobs
        .next(Duration::from_millis(100))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(consumed.job, job);
    assert!(jobs.acknowledge(&consumed.entry_id).await.unwrap());

    for event in [
        Event::AttemptStarted(AttemptStarted {}),
        Event::Token(TokenChunk {
            text: "hello ".into(),
            cumulative_generated_tokens: 1,
        }),
        Event::Token(TokenChunk {
            text: "world".into(),
            cumulative_generated_tokens: 2,
        }),
        Event::Completed(GenerationCompleted {
            prompt_tokens: 1,
            generated_tokens: 2,
            finish_reason: "stop".into(),
        }),
    ] {
        streams
            .publish_result(&GenerationResult {
                schema_version: SCHEMA_VERSION,
                request_id: job.request_id.clone(),
                attempt_id: "attempt-1".into(),
                event: Some(event),
            })
            .await
            .unwrap();
    }

    let mut first = streams
        .subscribe(&job.request_id, None)
        .await
        .unwrap()
        .unwrap();
    let mut second = streams
        .subscribe(&job.request_id, None)
        .await
        .unwrap()
        .unwrap();
    let mut first_ids = Vec::new();
    let mut second_ids = Vec::new();
    for _ in 0..4 {
        first_ids.push(
            first
                .next(Duration::from_millis(100))
                .await
                .unwrap()
                .unwrap()
                .event_id,
        );
        second_ids.push(
            second
                .next(Duration::from_millis(100))
                .await
                .unwrap()
                .unwrap()
                .event_id,
        );
    }
    assert_eq!(first_ids, second_ids);

    let mut resumed = streams
        .subscribe(&job.request_id, Some(&first_ids[1]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        resumed
            .next(Duration::from_millis(100))
            .await
            .unwrap()
            .unwrap()
            .event_id,
        first_ids[2]
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(
        streams
            .subscribe(&job.request_id, None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn quota_is_refunded_by_actual_generation() {
    let Some(streams) = test_client(2).await else {
        return;
    };
    let reservation = streams
        .reserve_quota(
            "api-key",
            10,
            100,
            QuotaLimits {
                requests_per_minute: 1,
                prompt_tokens_per_minute: 10,
                generated_tokens_per_minute: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        streams.reconcile_quota(&reservation.id, 25).await.unwrap(),
        75
    );
    assert_eq!(
        streams.reconcile_quota(&reservation.id, 90).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn stale_jobs_are_reclaimed_once_and_leases_deduplicate_workers() {
    let Some(streams) = test_client(3).await else {
        return;
    };
    let job = GenerationJob {
        schema_version: SCHEMA_VERSION,
        request_id: "request-reclaim".into(),
        model_id: "model".into(),
        request_json: b"{}".to_vec(),
        ..Default::default()
    };
    streams.publish_generation(&job).await.unwrap();
    let mut crashed = streams.consumer("engine-crashed").await.unwrap();
    let pending = crashed
        .next(Duration::from_millis(100))
        .await
        .unwrap()
        .unwrap();

    assert!(
        streams
            .acquire_lease(&job.request_id, "attempt-1")
            .await
            .unwrap()
    );
    assert!(
        !streams
            .acquire_lease(&job.request_id, "attempt-2")
            .await
            .unwrap()
    );
    assert!(
        streams
            .renew_lease(&job.request_id, "attempt-1")
            .await
            .unwrap()
    );
    streams
        .release_lease(&job.request_id, "attempt-1")
        .await
        .unwrap();
    assert!(
        streams
            .acquire_lease(&job.request_id, "attempt-2")
            .await
            .unwrap()
    );
    streams
        .release_lease(&job.request_id, "attempt-2")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(75)).await;
    let mut recovered = streams.consumer("engine-recovered").await.unwrap();
    let reclaimed = recovered
        .reclaim_stale(Duration::from_millis(50))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.entry_id, pending.entry_id);
    assert_eq!(reclaimed.job, job);
    assert!(recovered.acknowledge(&reclaimed.entry_id).await.unwrap());
    assert!(
        recovered
            .reclaim_stale(Duration::from_millis(0))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancellation_refunds_the_entire_reservation() {
    let Some(streams) = test_client(4).await else {
        return;
    };
    let limits = QuotaLimits {
        requests_per_minute: 1,
        prompt_tokens_per_minute: 10,
        generated_tokens_per_minute: 100,
    };
    let reservation = streams
        .reserve_quota("cancel-key", 10, 100, limits)
        .await
        .unwrap();
    streams.cancel_quota(&reservation.id).await.unwrap();
    streams
        .reserve_quota("cancel-key", 10, 100, limits)
        .await
        .unwrap();
}
