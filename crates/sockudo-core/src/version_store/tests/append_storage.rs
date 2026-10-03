//! Append storage representation: every original operation is retained,
//! the public full-state record is reconstructed exactly, and accumulated
//! strings are not retained once per version.
use super::super::append_storage::{
    APPEND_STORAGE_FORMAT, AppendRunPlan, AppendRunRef, StoredVersionPayload, encode_full,
};
use super::*;
use crate::message_envelope::{MessageContent, MessageEnvelope, PublishIdempotencyMetadata};

const APP: &str = "app";
const CHANNEL: &str = "chat";

fn serial(n: u64) -> VersionSerial {
    VersionSerial::new(format!("ver:{n:020}")).unwrap()
}

fn meta(n: u64) -> VersionMetadata {
    VersionMetadata {
        serial: serial(n),
        client_id: Some("agent".to_string()),
        timestamp_ms: n as i64,
        description: Some(format!("op {n}")),
        metadata: Some(sonic_rs::json!({"n": n})),
    }
}

/// Deterministic fragments mixing 1-, 2- and 4-byte characters.
fn fragment(n: u64, bytes: usize) -> String {
    let mut out = String::new();
    let mut state = n.wrapping_mul(0x9e3779b97f4a7c15) | 1;
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let left = bytes - out.len();
        match state % 8 {
            0 if left >= 4 => out.push('\u{1F642}'),
            1 if left >= 2 => out.push('\u{e9}'),
            value => out.push((b'a' + value as u8) as char),
        }
    }
    out
}

fn create(message: &str) -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: APP.to_string(),
        channel: CHANNEL.to_string(),
        original_client_id: Some("agent".to_string()),
        envelope: Some(MessageEnvelope {
            message_id: Some(format!("{message}:client")),
            name: Some("ai.response".to_string()),
            data: Some(MessageContent::Text("hi".to_string())),
            publisher_client_id: Some("agent".to_string()),
            published_at_ms: Some(1),
            ..MessageEnvelope::default()
        }),
        message: VersionedMessage::new_create(
            MessageSerial::new(message).unwrap(),
            meta(0),
            7,
            0,
            Some("ai.response".to_string()),
            Some(MessageData::String("hi".to_string())),
            None,
        ),
    }
}

fn json(record: &StoredVersionRecord) -> Vec<u8> {
    sonic_rs::to_vec(record).unwrap()
}

fn append_request(
    message: &str,
    current: &StoredVersionRecord,
    n: u64,
    data_fragment: String,
) -> VersionMutationRequest {
    VersionMutationRequest {
        app_id: APP.to_string(),
        channel: CHANNEL.to_string(),
        message_serial: MessageSerial::new(message).unwrap(),
        expected: VersionPrecondition::from_record(current),
        version: meta(n),
        mutation: VersionMutation::Append(MessageAppend {
            data_fragment,
            extras: None,
        }),
        idempotency: None,
        limits: VersionMutationLimits::default(),
    }
}

async fn commit(store: &MemoryVersionStore, record: StoredVersionRecord) -> StoredVersionRecord {
    match store
        .commit_create(VersionCreateRequest {
            record,
            limits: VersionCreateLimits::default(),
        })
        .await
        .unwrap()
    {
        VersionCreateResult::Applied { record, .. } => record,
        other => panic!("create was not applied: {other:?}"),
    }
}

async fn apply(store: &MemoryVersionStore, request: VersionMutationRequest) -> StoredVersionRecord {
    match store.compare_and_apply(request).await.unwrap() {
        VersionMutationResult::Applied { record, .. } => record,
        other => panic!("mutation was not applied: {other:?}"),
    }
}

async fn all_versions(store: &MemoryVersionStore, message: &str) -> Vec<StoredVersionRecord> {
    let mut cursor = None;
    let mut items = Vec::new();
    loop {
        let page = store
            .get_versions(VersionStoreReadRequest {
                app_id: APP.to_string(),
                channel: CHANNEL.to_string(),
                message_serial: MessageSerial::new(message).unwrap(),
                direction: VersionStoreDirection::OldestFirst,
                limit: 37,
                cursor,
            })
            .await
            .unwrap();
        items.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            return items;
        }
    }
}

fn append_record(n: u64) -> StoredVersionRecord {
    let current = create("msg:codec");
    let request = append_request("msg:codec", &current, n, fragment(n, 33));
    current.apply_mutation(&request, "stream", n).unwrap()
}

#[test]
fn compact_payload_round_trips_to_the_exact_full_record() {
    let record = append_record(1);
    let plan = AppendRunPlan::for_record(&serial(0), None, &record);
    let AppendRunPlan::Start { run } = &plan else {
        panic!("first append must start a run: {plan:?}");
    };
    assert_eq!(&run.run, record.version_serial());
    let bytes = plan.encode(&record).unwrap();
    assert!(bytes.len() < encode_full(&record).unwrap().len());
    let decoded = StoredVersionPayload::decode(&bytes).unwrap();
    assert_eq!(decoded.run(), Some(run));
    assert!(decoded.record().message.data.is_none());
    assert_eq!(
        decoded.record().message.append_fragment,
        record.message.append_fragment
    );
    let snapshot = plan.snapshot_after(&record).unwrap().to_string();
    let restored = decoded.into_record(Some(&snapshot)).unwrap();
    assert_eq!(json(&restored), json(&record));
}

#[test]
fn legacy_full_payloads_decode_unchanged() {
    let record = append_record(3);
    let bytes = encode_full(&record).unwrap();
    // Byte-identical to what earlier releases persisted.
    assert_eq!(bytes, sonic_rs::to_vec(&record).unwrap());
    let decoded = StoredVersionPayload::decode(&bytes).unwrap();
    assert!(decoded.run().is_none());
    assert_eq!(json(&decoded.into_record(None).unwrap()), json(&record));
}

#[test]
fn chunked_record_aliases_preserve_earlier_rows_and_nested_user_metadata() {
    let mut record = append_record(3);
    record.message.version.metadata = Some(sonic_rs::json!({
        "app_id": "user value", "a": null, "message": {"m": [1, null, "é"]}
    }));
    let legacy = AppendRunPlan::for_record(&serial(0), None, &record);
    let compact_record = super::super::append_storage::without_data(&record);
    let legacy_run = legacy.run().unwrap();
    let record_json = sonic_rs::to_string(&compact_record).unwrap();
    let expected_legacy = format!(
        r#"{{"sockudo_append_storage":1,"run":{},"data_len":{},"record":{}}}"#,
        sonic_rs::to_string(&legacy_run.run).unwrap(),
        legacy_run.data_len,
        record_json
    );
    assert_eq!(legacy.encode(&record).unwrap(), expected_legacy.as_bytes());

    let plan = AppendRunPlan::for_record_chunked(&serial(0), None, &record);
    let run = plan.run().unwrap();
    let earlier_chunked = format!(
        r#"{{"sockudo_append_storage":2,"run":{},"data_len":{},"generation":{},"record":{}}}"#,
        sonic_rs::to_string(&run.run).unwrap(),
        run.data_len,
        sonic_rs::to_string(run.generation.as_ref().unwrap()).unwrap(),
        record_json
    );
    let current = plan.encode(&record).unwrap();
    assert!(current.len() < earlier_chunked.len());
    let snapshot = plan.snapshot_after(&record).unwrap();
    for bytes in [current.as_slice(), earlier_chunked.as_bytes()] {
        let decoded = StoredVersionPayload::decode(bytes).unwrap();
        assert_eq!(decoded.run(), Some(run));
        assert_eq!(
            json(&decoded.into_record(Some(snapshot)).unwrap()),
            json(&record)
        );
    }
}

#[test]
fn older_readers_reject_compact_payloads_instead_of_losing_data() {
    let record = append_record(4);
    let plan = AppendRunPlan::for_record(&serial(0), None, &record);
    let bytes = plan.encode(&record).unwrap();
    // The pre-change decoder: a compact payload must not decode as a record.
    assert!(sonic_rs::from_slice::<StoredVersionRecord>(&bytes).is_err());
}

#[test]
fn reconstruction_fails_closed() {
    let record = append_record(5);
    let plan = AppendRunPlan::for_record(&serial(0), None, &record);
    let bytes = plan.encode(&record).unwrap();
    let snapshot = plan.snapshot_after(&record).unwrap().to_string();
    let decoded = || StoredVersionPayload::decode(&bytes).unwrap();

    let missing = decoded().into_record(None).unwrap_err();
    assert!(missing.to_string().contains("missing"), "{missing}");

    let short = "z".repeat(snapshot.len() - 1);
    assert!(decoded().into_record(Some(&short)).is_err());

    // Same length, but the snapshot does not hold the entry's fragment.
    let foreign = "z".repeat(snapshot.len());
    assert!(decoded().into_record(Some(&foreign)).is_err());

    // A length inside a multi-byte character is rejected.
    let mut record = append_record(6);
    record.message.data = Some(MessageData::String("\u{1F642}".to_string()));
    record.message.append_fragment = Some("\u{1F642}".to_string());
    if let Some(envelope) = record.envelope.as_mut() {
        envelope.data = Some(MessageContent::Text("\u{1F642}".to_string()));
    }
    let bad = StoredVersionPayload::Compact {
        run: AppendRunRef {
            generation: None,
            run: serial(6),
            data_len: 2,
        },
        record: super::super::append_storage::without_data(&record),
    };
    assert!(bad.into_record(Some("\u{1F642}")).is_err());

    // Future formats and malformed compact records are refused.
    let future = String::from_utf8(bytes.clone()).unwrap().replacen(
        &format!("\"sockudo_append_storage\":{APPEND_STORAGE_FORMAT}"),
        "\"sockudo_append_storage\":9",
        1,
    );
    assert!(StoredVersionPayload::decode(future.as_bytes()).is_err());
    let with_data = StoredVersionPayload::Compact {
        run: AppendRunRef {
            generation: None,
            run: serial(5),
            data_len: 1,
        },
        record: append_record(5),
    }
    .encode()
    .unwrap();
    assert!(StoredVersionPayload::decode(&with_data).is_err());
}

#[test]
fn non_append_and_non_string_records_stay_self_contained() {
    let current = create("msg:codec");
    assert_eq!(
        AppendRunPlan::for_record(&serial(0), None, &current),
        AppendRunPlan::Full
    );
    let update = current
        .apply_mutation(
            &VersionMutationRequest {
                mutation: VersionMutation::Update(MessageFieldDelta::default()),
                ..append_request("msg:codec", &current, 1, String::new())
            },
            "stream",
            1,
        )
        .unwrap();
    assert_eq!(
        AppendRunPlan::for_record(&serial(0), None, &update),
        AppendRunPlan::Full
    );
    // Envelope data that is not the accumulated text is never factored out.
    let mut odd = append_record(2);
    if let Some(envelope) = odd.envelope.as_mut() {
        envelope.data = Some(MessageContent::Binary(vec![1, 2]));
    }
    assert_eq!(
        AppendRunPlan::for_record(&serial(0), None, &odd),
        AppendRunPlan::Full
    );
}

#[tokio::test]
async fn long_stream_reads_equal_the_committed_full_records() {
    for (appends, bytes) in [(128u64, 16usize), (512, 64), (2000, 64)] {
        let store = MemoryVersionStore::new();
        let mut expected = vec![commit(&store, create("msg:1")).await];
        for n in 1..=appends {
            let current = expected.last().unwrap();
            let next = apply(
                &store,
                append_request("msg:1", current, n, fragment(n, bytes)),
            )
            .await;
            expected.push(next);
        }

        let versions = all_versions(&store, "msg:1").await;
        assert_eq!(versions.len(), expected.len());
        for (read, committed) in versions.iter().zip(&expected) {
            assert_eq!(json(read), json(committed));
        }
        let mut replay = Vec::new();
        let mut after = 0;
        loop {
            let page = store
                .replay_after(VersionReplayRequest {
                    app_id: APP.to_string(),
                    channel: CHANNEL.to_string(),
                    after_delivery_serial: after,
                    limit: 64,
                })
                .await
                .unwrap();
            let Some(last) = page.last() else { break };
            after = last.delivery_serial();
            replay.extend(page);
        }
        for (read, committed) in replay.iter().zip(&expected) {
            assert_eq!(json(read), json(committed));
        }
        let latest = store
            .get_latest(APP, CHANNEL, &MessageSerial::new("msg:1").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(json(&latest), json(expected.last().unwrap()));

        // Random historical reads through a cursor.
        for target in [1, appends / 3, appends / 2, appends - 1, appends] {
            let page = store
                .get_versions(VersionStoreReadRequest {
                    app_id: APP.to_string(),
                    channel: CHANNEL.to_string(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    direction: VersionStoreDirection::NewestFirst,
                    limit: 1,
                    cursor: Some(VersionStoreCursor {
                        version: 1,
                        version_serial: serial(target + 1),
                        direction: VersionStoreDirection::NewestFirst,
                    }),
                })
                .await
                .unwrap();
            assert_eq!(json(&page.items[0]), json(&expected[target as usize]));
        }

        // Every append is compact; one snapshot holds the accumulated data.
        let stats = store.append_storage_stats().await;
        assert_eq!(stats.compact_entries as u64, appends);
        assert_eq!(stats.full_entries, 1);
        assert_eq!(stats.runs, 1);
        assert_eq!(
            stats.snapshot_bytes,
            expected.last().unwrap().data_bytes().unwrap()
        );
        assert_eq!(stats.entry_data_bytes, "hi".len());
    }
}

#[tokio::test]
async fn updates_close_runs_and_later_appends_start_new_runs() {
    let store = MemoryVersionStore::new();
    let mut expected = vec![commit(&store, create("msg:1")).await];
    let mut n = 0;
    for round in 0..3u64 {
        for _ in 0..20 {
            n += 1;
            let current = expected.last().unwrap().clone();
            expected.push(
                apply(
                    &store,
                    append_request("msg:1", &current, n, fragment(n, 11)),
                )
                .await,
            );
        }
        n += 1;
        let current = expected.last().unwrap().clone();
        let delta = MessageFieldDelta {
            data: if round == 1 {
                FieldPatch::Replace(MessageData::String(format!("reset {round}")))
            } else {
                FieldPatch::Keep
            },
            ..MessageFieldDelta::default()
        };
        expected.push(
            apply(
                &store,
                VersionMutationRequest {
                    mutation: VersionMutation::Update(delta),
                    ..append_request("msg:1", &current, n, String::new())
                },
            )
            .await,
        );
    }
    let versions = all_versions(&store, "msg:1").await;
    assert_eq!(versions.len(), expected.len());
    for (read, committed) in versions.iter().zip(&expected) {
        assert_eq!(json(read), json(committed));
    }
    let stats = store.append_storage_stats().await;
    assert_eq!(stats.runs, 3);
    assert_eq!(stats.compact_entries, 60);
    assert_eq!(stats.full_entries, 4);
}

#[tokio::test]
async fn idempotent_replay_of_a_compact_append_returns_the_original_record() {
    let store = MemoryVersionStore::new();
    let created = commit(&store, create("msg:1")).await;
    let first = apply(
        &store,
        append_request("msg:1", &created, 1, "alpha".to_string()),
    )
    .await;
    let receipt = PublishIdempotencyMetadata {
        cache_key: "op-2".to_string(),
        payload_fingerprint: "fingerprint".to_string(),
    };
    let request = VersionMutationRequest {
        idempotency: Some(receipt.clone()),
        ..append_request("msg:1", &first, 2, "beta".to_string())
    };
    let applied = apply(&store, request.clone()).await;
    // Later appends keep extending the run the receipt points into.
    let third = apply(
        &store,
        append_request("msg:1", &applied, 3, "gamma".to_string()),
    )
    .await;
    assert_eq!(
        third
            .message
            .data
            .clone()
            .and_then(MessageData::into_string)
            .as_deref(),
        Some("hialphabetagamma")
    );

    let VersionMutationResult::Duplicate { record, .. } =
        store.compare_and_apply(request.clone()).await.unwrap()
    else {
        panic!("replay must be a duplicate");
    };
    assert_eq!(json(&record), json(&applied));

    let conflicting = VersionMutationRequest {
        idempotency: Some(PublishIdempotencyMetadata {
            payload_fingerprint: "different".to_string(),
            ..receipt
        }),
        ..request
    };
    assert!(matches!(
        store.compare_and_apply(conflicting).await,
        Err(crate::error::Error::IdempotencyConflict)
    ));
}

#[tokio::test]
async fn terminal_state_and_limits_apply_to_compact_latest_versions() {
    let store = MemoryVersionStore::new();
    let created = commit(&store, create("msg:1")).await;
    let mut current = created;
    for n in 1..=3 {
        current = apply(
            &store,
            append_request("msg:1", &current, n, "abc".to_string()),
        )
        .await;
    }
    let limited = |current: &StoredVersionRecord, n, limits| VersionMutationRequest {
        limits,
        ..append_request("msg:1", current, n, "abc".to_string())
    };
    // Accumulated bytes are checked against the reconstructed aggregate.
    let over = limited(
        &current,
        4,
        VersionMutationLimits {
            max_accumulated_message_bytes: Some("hi".len() + 3 * 3 + 2),
            ..VersionMutationLimits::default()
        },
    );
    assert!(matches!(
        store.compare_and_apply(over).await.unwrap(),
        VersionMutationResult::Rejected(VersionMutationRejection::AccumulatedMessageBytes { .. })
    ));
    let over = limited(
        &current,
        4,
        VersionMutationLimits {
            max_appends_per_message: Some(3),
            ..VersionMutationLimits::default()
        },
    );
    assert!(matches!(
        store.compare_and_apply(over).await.unwrap(),
        VersionMutationResult::Rejected(VersionMutationRejection::AppendCount { limit: 3 })
    ));

    // A terminal append persists its status in the compact latest version.
    let extras = sockudo_protocol::messages::MessageExtras {
        ai: Some(AiExtras {
            opaque: Default::default(),
            transport: Some(HashMap::from([(
                "status".to_string(),
                "complete".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    };
    let terminal = apply(
        &store,
        VersionMutationRequest {
            mutation: VersionMutation::Append(MessageAppend {
                data_fragment: "!".to_string(),
                extras: Some(extras),
            }),
            ..append_request("msg:1", &current, 4, String::new())
        },
    )
    .await;
    let latest = store
        .get_latest(APP, CHANNEL, &MessageSerial::new("msg:1").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(json(&latest), json(&terminal));
    let after_terminal = limited(
        &terminal,
        5,
        VersionMutationLimits {
            reject_append_after_terminal: true,
            ..VersionMutationLimits::default()
        },
    );
    assert!(matches!(
        store.compare_and_apply(after_terminal).await.unwrap(),
        VersionMutationResult::Rejected(VersionMutationRejection::TerminalMessage)
    ));
}

#[tokio::test]
async fn purge_keeps_retained_versions_readable_and_frees_unreferenced_runs() {
    let store = MemoryVersionStore::new();
    let mut expected = vec![commit(&store, create("msg:1")).await];
    for n in 1..=50 {
        let current = expected.last().unwrap().clone();
        expected.push(apply(&store, append_request("msg:1", &current, n, fragment(n, 9))).await);
    }
    // Purge by creation time removes the oldest entries first; bound the
    // batch so only the create and the first 30 appends go.
    let (deleted, has_more) = store.purge_before(i64::MAX, 31).await.unwrap();
    assert_eq!(deleted, 31);
    assert!(has_more);
    let versions = all_versions(&store, "msg:1").await;
    assert_eq!(versions.len(), 20);
    for (read, committed) in versions.iter().zip(&expected[31..]) {
        assert_eq!(json(read), json(committed));
    }
    assert_eq!(store.append_storage_stats().await.runs, 1);

    store.purge_before(i64::MAX, 1000).await.unwrap();
    assert!(all_versions(&store, "msg:1").await.is_empty());
    assert_eq!(
        store.append_storage_stats().await,
        super::super::memory::AppendStorageStats::default()
    );
}

#[tokio::test]
async fn imported_full_versions_are_read_back_and_break_runs() {
    let store = MemoryVersionStore::new();
    let created = commit(&store, create("msg:1")).await;
    let first = apply(
        &store,
        append_request("msg:1", &created, 1, "one".to_string()),
    )
    .await;
    // An import (e.g. from an older release) stores a self-contained append.
    let import_request = append_request("msg:1", &first, 2, "two".to_string());
    let imported = first
        .apply_mutation(
            &import_request,
            &first.envelope.clone().unwrap().stream_id.unwrap(),
            3,
        )
        .unwrap();
    store.append_version(imported.clone()).await.unwrap();
    let after = apply(
        &store,
        append_request("msg:1", &imported, 3, "three".to_string()),
    )
    .await;
    let versions = all_versions(&store, "msg:1").await;
    assert_eq!(
        versions.iter().map(json).collect::<Vec<_>>(),
        [&created, &first, &imported, &after]
            .into_iter()
            .map(json)
            .collect::<Vec<_>>()
    );
    let stats = store.append_storage_stats().await;
    assert_eq!(stats.runs, 2);
    assert_eq!(stats.full_entries, 2);
}

#[tokio::test]
async fn replay_across_messages_reconstructs_each_run() {
    let store = MemoryVersionStore::new();
    let mut expected = Vec::new();
    let mut latest = HashMap::new();
    for message in ["msg:a", "msg:b"] {
        let record = commit(&store, create(message)).await;
        expected.push(record.clone());
        latest.insert(message, record);
    }
    for n in 1..=30u64 {
        let message = if n % 3 == 0 { "msg:b" } else { "msg:a" };
        let current = latest[message].clone();
        let next = apply(&store, append_request(message, &current, n, fragment(n, 5))).await;
        expected.push(next.clone());
        latest.insert(message, next);
    }
    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: APP.to_string(),
            channel: CHANNEL.to_string(),
            after_delivery_serial: 0,
            limit: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        replay.iter().map(json).collect::<Vec<_>>(),
        expected.iter().map(json).collect::<Vec<_>>()
    );
    let projected = store.latest_by_history(APP, CHANNEL).await.unwrap();
    assert_eq!(projected.len(), 2);
    for record in projected {
        let message = record.message_serial().as_str().to_string();
        assert_eq!(json(&record), json(&latest[message.as_str()]));
    }
}

#[test]
fn chunked_runs_bound_tail_writes_and_preserve_utf8_across_boundaries() {
    use super::super::append_storage::CHUNK_BYTES;
    let mut current = create("msg:chunks");
    let mut previous_run = None;
    let mut chunks = std::collections::BTreeMap::new();
    let fragments = [
        "x".repeat(CHUNK_BYTES - 3),
        "🙂".to_owned(),
        "é".repeat(CHUNK_BYTES),
        "z".repeat(CHUNK_BYTES),
        String::new(),
    ];
    for (n, fragment) in fragments.into_iter().enumerate() {
        let record = current
            .apply_mutation(
                &append_request("msg:chunks", &current, n as u64 + 1, fragment.clone()),
                "stream",
                n as u64 + 1,
            )
            .unwrap();
        let plan = AppendRunPlan::for_record_chunked(
            current.version_serial(),
            previous_run.as_ref(),
            &record,
        );
        let writes = plan.chunk_writes(&record).unwrap();
        assert!(writes.iter().all(|chunk| chunk.bytes.len() <= CHUNK_BYTES));
        assert!(
            writes.iter().map(|chunk| chunk.bytes.len()).sum::<usize>()
                <= fragment.len() + CHUNK_BYTES
        );
        for chunk in writes {
            chunks.insert(chunk.index, chunk.bytes);
        }
        let bytes = chunks.values().flatten().copied().collect::<Vec<_>>();
        let data = String::from_utf8(bytes).unwrap();
        let encoded = plan.encode(&record).unwrap();
        assert!(encoded.starts_with(br#"{"sockudo_append_storage":2,"#));
        assert!(sonic_rs::from_slice::<StoredVersionRecord>(&encoded).is_err());
        assert_eq!(
            json(
                &StoredVersionPayload::decode(&encoded)
                    .unwrap()
                    .into_record(Some(&data))
                    .unwrap()
            ),
            json(&record)
        );
        previous_run = plan.run().cloned();
        current = record;
    }
}

#[test]
fn format2_requires_generation_and_format1_stays_readable() {
    let record = append_record(1);
    let old = AppendRunPlan::for_record(&serial(0), None, &record);
    let old_bytes = old.encode(&record).unwrap();
    assert!(StoredVersionPayload::decode(&old_bytes).is_ok());
    let mut value: serde_json::Value = serde_json::from_slice(&old_bytes).unwrap();
    value["sockudo_append_storage"] = 2.into();
    // Preserve discriminator first, as all storage writers do.
    let invalid = String::from_utf8(old_bytes)
        .unwrap()
        .replacen("storage\":1", "storage\":2", 1);
    assert!(StoredVersionPayload::decode(invalid.as_bytes()).is_err());
    let new = AppendRunPlan::for_record_chunked(&serial(1), old.run(), &record);
    assert!(matches!(new, AppendRunPlan::Start { .. }));
    assert!(new.run().unwrap().generation.is_some());
    let replacement = AppendRunPlan::for_record_chunked(&serial(1), None, &record);
    assert_ne!(
        replacement.run().unwrap().generation,
        new.run().unwrap().generation
    );
}

#[test]
fn snapshot_cache_is_bounded_and_isolates_replaced_runs_and_tenants() {
    use super::super::append_storage::AppendSnapshotCache;
    let cache = AppendSnapshotCache::new(1024);
    let record = append_record(1);
    let plan = AppendRunPlan::for_record_chunked(&serial(0), None, &record);
    let run = plan.run().unwrap();
    let data = plan.snapshot_after(&record).unwrap();
    let message = record.message_serial();
    cache.insert(APP, CHANNEL, message, run, data.to_owned());
    assert_eq!(cache.get(APP, CHANNEL, message, run).as_deref(), Some(data));
    assert!(cache.get("other", CHANNEL, message, run).is_none());
    let mut replaced = run.clone();
    replaced.generation = Some(uuid::Uuid::new_v4().to_string());
    assert!(cache.get(APP, CHANNEL, message, &replaced).is_none());
    let mut extended = run.clone();
    extended.data_len += 2;
    assert!(cache.get(APP, CHANNEL, message, &extended).is_none());
    cache.insert(APP, CHANNEL, message, &extended, format!("{data}é"));
    assert_eq!(cache.get(APP, CHANNEL, message, run).as_deref(), Some(data));
    for _ in 0..10 {
        replaced.generation = Some(uuid::Uuid::new_v4().to_string());
        cache.insert(APP, CHANNEL, message, &replaced, data.to_owned());
    }
    assert!(cache.get(APP, CHANNEL, message, run).is_none());
    let mut reserved = String::with_capacity(8192);
    reserved.push_str(data);
    cache.insert(APP, CHANNEL, message, run, reserved);
    assert!(cache.get(APP, CHANNEL, message, run).is_none());
    cache.insert(APP, CHANNEL, message, run, "x".repeat(2048));
    assert!(cache.get(APP, CHANNEL, message, run).is_none());
    let mut legacy = run.clone();
    legacy.generation = None;
    cache.insert(APP, CHANNEL, message, &legacy, data.to_owned());
    assert!(cache.get(APP, CHANNEL, message, &legacy).is_none());
}
