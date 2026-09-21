"""End-to-end API behavior: causality, gaps, withdrawals, idempotency."""


def _event(rev, parent, start, end, value=None, withdrawn=False, key=None,
           batch="B1", sensor="S1", unit="mm"):
    return {
        "batch_id": batch,
        "sensor_id": sensor,
        "revision_id": rev,
        "parent_id": parent,
        "idempotency_key": key or f"k-{rev}",
        "start_ns": start,
        "end_ns": end,
        "value": value,
        "unit": unit,
        "withdrawn": withdrawn,
    }


def _post(client, event, batch="B1"):
    return client.post(f"/api/v1/batches/{batch}/events", json=event)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["storage"] == "memory"


def test_out_of_order_root_then_child(client):
    # Child arrives first: blocked and reported as a gap.
    r = _post(client, _event("v2", "v1", 10, 20, value=2.0))
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "accepted"
    assert body["result"]["sensors"][0]["segments"] == []
    assert body["result"]["complete"] is False
    gap = body["result"]["gaps"][0]
    assert gap["missing_revision_id"] == "v1"
    assert gap["awaited_by"] == ["v2"]

    # Root backfilled: timeline becomes grounded and complete.
    r = _post(client, _event("v1", "ROOT", 0, 100, value=1.0))
    assert r.status_code == 200
    result = r.json()["result"]
    assert result["complete"] is True
    segs = result["sensors"][0]["segments"]
    assert [(s["start_ns"], s["end_ns"], s["value"], s["revision_id"]) for s in segs] == [
        (0, 10, 1.0, "v1"),
        (10, 20, 2.0, "v2"),
        (20, 100, 1.0, "v1"),
    ]


def test_overlapping_child_overwrites_parent_interval(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    r = _post(client, _event("c", "r", 40, 60, value=9.0))
    segs = r.json()["result"]["sensors"][0]["segments"]
    assert [(s["start_ns"], s["end_ns"], s["value"]) for s in segs] == [
        (0, 40, 5.0),
        (40, 60, 9.0),
        (60, 100, 5.0),
    ]


def test_withdrawn_revision_erases_window(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    r = _post(client, _event("w", "r", 30, 70, withdrawn=True, value=None))
    segs = r.json()["result"]["sensors"][0]["segments"]
    assert [(s["start_ns"], s["end_ns"], s["state"], s["value"]) for s in segs] == [
        (0, 30, "calibrated", 5.0),
        (30, 70, "withdrawn", None),
        (70, 100, "calibrated", 5.0),
    ]


def test_withdrawn_can_be_superseded_by_descendant(client):
    _post(client, _event("r", "ROOT", 0, 100, value=5.0))
    _post(client, _event("w", "r", 0, 100, withdrawn=True, value=None))
    r = _post(client, _event("fix", "w", 10, 90, value=7.0))
    segs = r.json()["result"]["sensors"][0]["segments"]
    assert [(s["start_ns"], s["end_ns"], s["state"], s["value"]) for s in segs] == [
        (0, 10, "withdrawn", None),
        (10, 90, "calibrated", 7.0),
        (90, 100, "withdrawn", None),
    ]


def test_duplicate_event_is_idempotent(client):
    event = _event("v1", "ROOT", 0, 10, value=1.0)
    r1 = _post(client, event)
    h1 = r1.json()["result"]["timeline_hash"]
    r2 = _post(client, event)
    assert r2.json()["status"] == "duplicate"
    assert r2.json()["result"]["timeline_hash"] == h1
    assert r2.json()["result"]["revisions_total"] == 1


def test_same_key_different_payload_conflicts(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, key="same"))
    r = _post(client, _event("v2", "ROOT", 0, 10, value=2.0, key="same"))
    assert r.status_code == 409


def test_same_revision_different_content_conflicts(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, key="k1"))
    r = _post(client, _event("v1", "ROOT", 0, 10, value=9.0, key="k2"))
    assert r.status_code == 409


def test_missing_chain_reports_only_direct_missing_parent(client):
    # v3 -> v2 -> [missing v1]; also v4 -> v2.
    _post(client, _event("v3", "v2", 0, 10, value=3.0, key="k3"))
    _post(client, _event("v4", "v2", 5, 15, value=4.0, key="k4"))
    r = client.get("/api/v1/batches/B1/result")
    gaps = r.json()["gaps"]
    # v2 has not been uploaded yet, so it is itself the backfillable gap.
    assert [(g["missing_revision_id"], sorted(g["awaited_by"])) for g in gaps] == [
        ("v2", ["v3", "v4"]),
    ]
    # Once v2 arrives the gap collapses to the still-missing v1.
    _post(client, _event("v2", "v1", 0, 20, value=2.0, key="k2"))
    r = client.get("/api/v1/batches/B1/result")
    assert [(g["missing_revision_id"], g["awaited_by"]) for g in r.json()["gaps"]] == [
        ("v1", ["v2"]),
    ]


def test_backfill_makes_batch_complete(client):
    _post(client, _event("v3", "v2", 0, 10, value=3.0))
    _post(client, _event("v2", "v1", 0, 20, value=2.0))
    res = client.get("/api/v1/batches/B1/result").json()
    assert res["revisions_blocked"] == 2
    assert res["revisions_grounded"] == 0
    r = _post(client, _event("v1", "ROOT", 0, 30, value=1.0))
    assert r.json()["result"]["complete"] is True
    assert r.json()["result"]["revisions_grounded"] == 3


def test_determinism_across_arrival_permutations(client):
    def submit_all(events):
        client.post("/admin/reset")
        hashes = []
        for e in events:
            resp = _post(client, e)
            assert resp.status_code == 200
        return client.get("/api/v1/batches/B1/result").json()["timeline_hash"]

    events = [
        _event("r", "ROOT", 0, 100, value=1.0),
        _event("a", "r", 0, 50, value=2.0, key="ka"),
        _event("b", "r", 25, 75, value=3.0, key="kb"),
        _event("c", "a", 40, 60, withdrawn=True, value=None, key="kc"),
    ]
    h_forward = submit_all(events)
    h_reverse = submit_all(list(reversed(events)))
    h_shuffled = submit_all([events[2], events[0], events[3], events[1]])
    assert h_forward == h_reverse == h_shuffled


def test_multiple_sensors_are_independent(client):
    _post(client, _event("x", "ROOT", 0, 10, value=1.0, sensor="SX", key="kx"))
    r = _post(client, _event("y", "missing", 0, 10, value=2.0, sensor="SY", key="ky"))
    result = r.json()["result"]
    sensors = {s["sensor_id"]: s for s in result["sensors"]}
    assert sensors["SX"]["segments"][0]["value"] == 1.0
    assert sensors["SY"]["segments"] == []
    assert result["gaps"][0]["sensor_id"] == "SY"


def test_reset_clears_everything(client):
    _post(client, _event("r", "ROOT", 0, 10, value=1.0))
    r = client.post("/admin/reset")
    assert r.json()["events_removed"] == 1
    assert client.get("/api/v1/batches/B1/result").status_code == 404


def test_validation_errors(client):
    bad_interval = _event("r", "ROOT", 50, 40, value=1.0)
    assert _post(client, bad_interval).status_code == 422
    missing_value = _event("r2", "ROOT", 0, 10)
    assert _post(client, missing_value).status_code == 422
    reserved = _event("ROOT", "ROOT", 0, 10, value=1.0, key="kr")
    assert _post(client, reserved).status_code == 422
    mismatch = _event("r3", "ROOT", 0, 10, value=1.0, batch="OTHER")
    assert client.post("/api/v1/batches/B1/events", json=mismatch).status_code == 422


def test_abutting_half_open_intervals_do_not_duplicate(client):
    _post(client, _event("a", "ROOT", 0, 10, value=1.0, key="a"))
    r = _post(client, _event("b", "a", 10, 20, value=2.0, key="b"))
    segs = r.json()["result"]["sensors"][0]["segments"]
    assert [(s["start_ns"], s["end_ns"]) for s in segs] == [(0, 10), (10, 20)]


# ---------------------------------------------------------------------------
# Sample-point lookup
# ---------------------------------------------------------------------------


def _sample(client, ts, batch="B1", sensor="S1"):
    return client.get(
        f"/api/v1/batches/{batch}/sensors/{sensor}/sample",
        params={"timestamp_ns": ts},
    )


def test_sample_calibrated_returns_value_unit_winner_and_parent_chain(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0, unit="mm"))
    _post(client, _event("v2", "v1", 10, 20, value=2.0))

    inside = _sample(client, 15).json()
    assert inside["state"] == "calibrated"
    assert inside["value"] == 2.0
    assert inside["unit"] == "mm"
    assert inside["revision_id"] == "v2"
    assert inside["parent_chain"] == ["v2", "v1", "ROOT"]
    assert inside["missing_ancestors"] == []

    outside_child = _sample(client, 50).json()
    assert outside_child["state"] == "calibrated"
    assert outside_child["value"] == 1.0
    assert outside_child["revision_id"] == "v1"
    assert outside_child["parent_chain"] == ["v1", "ROOT"]


def test_sample_half_open_boundaries(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0))
    _post(client, _event("v2", "v1", 10, 20, value=2.0))
    assert _sample(client, 10).json()["revision_id"] == "v2"
    # End boundary belongs to the enclosing revision.
    assert _sample(client, 20).json()["revision_id"] == "v1"


def test_sample_withdrawn_point(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0))
    _post(client, _event("v2", "v1", 0, 100, value=2.0))
    _post(client, _event("w", "v2", 30, 70, withdrawn=True, value=None))
    body = _sample(client, 50).json()
    assert body["state"] == "withdrawn"
    assert body["value"] is None
    assert body["unit"] is None
    assert body["revision_id"] == "w"
    assert body["parent_chain"] == ["w", "v2", "v1", "ROOT"]


def test_sample_uncovered_point(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0))
    body = _sample(client, 500).json()
    assert body["state"] == "uncovered"
    assert body["value"] is None
    assert body["revision_id"] is None
    assert body["parent_chain"] == []
    assert body["missing_ancestors"] == []


def test_sample_blocked_lists_missing_ancestor(client):
    _post(client, _event("v2", "v1", 10, 20, value=2.0))
    body = _sample(client, 15).json()
    assert body["state"] == "blocked"
    assert body["missing_ancestors"] == ["v1"]
    assert body["value"] is None
    assert body["revision_id"] is None
    assert body["parent_chain"] == []
    # An instant the blocked revision does not cover is simply uncovered.
    assert _sample(client, 99).json()["state"] == "uncovered"


def test_sample_blocked_takes_precedence_over_grounded_coverage(client):
    _post(client, _event("r", "ROOT", 0, 100, value=1.0))
    _post(client, _event("orphan", "ghost", 40, 60, value=9.0, key="ko"))
    body = _sample(client, 50).json()
    assert body["state"] == "blocked"
    assert body["missing_ancestors"] == ["ghost"]


def test_sample_blocked_collects_distinct_terminal_ancestors(client):
    # o1 -> g1 and o2 -> g2; both cover t=7, and a longer chain ends at v1.
    _post(client, _event("o1", "g1", 0, 10, value=1.0, key="ko1"))
    _post(client, _event("o2", "g2", 5, 15, value=2.0, key="ko2"))
    _post(client, _event("v3", "v2", 6, 8, value=3.0, key="kv3"))
    _post(client, _event("v2", "v1", 0, 9, value=2.5, key="kv2"))
    body = _sample(client, 7).json()
    assert body["state"] == "blocked"
    assert body["missing_ancestors"] == ["g1", "g2", "v1"]


def test_sample_blocked_resolves_after_backfill(client):
    _post(client, _event("v2", "v1", 10, 20, value=2.0))
    assert _sample(client, 15).json()["state"] == "blocked"
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0))
    body = _sample(client, 15).json()
    assert body["state"] == "calibrated"
    assert body["revision_id"] == "v2"
    assert body["parent_chain"] == ["v2", "v1", "ROOT"]


def test_sample_unknown_batch_or_sensor_404(client):
    assert _sample(client, 0, batch="NOPE").status_code == 404
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, sensor="SX", key="kx"))
    assert _sample(client, 0, sensor="SY").status_code == 404
    client.post("/admin/reset")
    assert _sample(client, 0).status_code == 404


def test_sample_rejects_negative_timestamp(client):
    r = client.get("/api/v1/batches/B1/sensors/S1/sample", params={"timestamp_ns": -1})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Cross-batch comparison
# ---------------------------------------------------------------------------


def _compare(client, left="L", right="R", sensor="S1"):
    return client.get(
        "/api/v1/batches/compare",
        params={
            "left_batch_id": left,
            "right_batch_id": right,
            "sensor_id": sensor,
        },
    )


def test_compare_identical_batches_all_equal(client):
    for batch in ("L", "R"):
        _post(client, _event("v1", "ROOT", 0, 100, value=1.0, batch=batch), batch=batch)
        _post(client, _event("v2", "v1", 10, 20, value=2.0, batch=batch), batch=batch)
    r = _compare(client)
    assert r.status_code == 200
    body = r.json()
    assert body["sensor_id"] == "S1"
    assert body["left_batch_id"] == "L" and body["right_batch_id"] == "R"
    assert body["incomplete"] is False
    assert body["incomplete_reasons"] == []
    assert body["left_segments_hash"] == body["right_segments_hash"]
    # Distinct values keep interval boundaries even within one kind.
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 10, "equal"),
        (10, 20, "equal"),
        (20, 100, "equal"),
    ]


def test_compare_merges_adjacent_intervals_across_revision_boundaries(client):
    # Left folds to two segments (different revisions, same effective value);
    # right folds to one. The diff merges them into a single equal interval.
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="L"), batch="L")
    _post(client, _event("v2", "v1", 10, 20, value=1.0, batch="L"), batch="L")
    _post(client, _event("w1", "ROOT", 0, 20, value=1.0, batch="R"), batch="R")
    body = _compare(client).json()
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 20, "equal")
    ]
    assert body["intervals"][0]["left"] == {"state": "calibrated", "value": 1.0, "unit": "mm"}
    assert body["intervals"][0]["right"] == {"state": "calibrated", "value": 1.0, "unit": "mm"}


def test_compare_value_mismatch(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0, batch="L"), batch="L")
    _post(client, _event("v1", "ROOT", 0, 100, value=2.0, batch="R"), batch="R")
    body = _compare(client).json()
    [interval] = body["intervals"]
    assert (interval["start_ns"], interval["end_ns"], interval["kind"]) == (
        0,
        100,
        "value_mismatch",
    )
    assert interval["left"]["value"] == 1.0
    assert interval["right"]["value"] == 2.0


def test_compare_unit_mismatch_counts_as_value_mismatch(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, unit="mm", batch="L"), batch="L")
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, unit="C", batch="R"), batch="R")
    body = _compare(client).json()
    assert [i["kind"] for i in body["intervals"]] == ["value_mismatch"]


def test_compare_state_mismatch(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0, batch="L"), batch="L")
    _post(
        client,
        _event("w1", "ROOT", 0, 100, withdrawn=True, value=None, batch="R"),
        batch="R",
    )
    body = _compare(client).json()
    [interval] = body["intervals"]
    assert interval["kind"] == "state_mismatch"
    assert interval["left"]["state"] == "calibrated"
    assert interval["right"] == {"state": "withdrawn", "value": None, "unit": None}


def test_compare_left_only_right_only_and_both_uncovered(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="L"), batch="L")
    _post(client, _event("w1", "ROOT", 20, 30, value=2.0, batch="R"), batch="R")
    body = _compare(client).json()
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 10, "left_only"),
        (10, 20, "equal"),
        (20, 30, "right_only"),
    ]
    left_only, both_uncovered, right_only = body["intervals"]
    assert left_only["left"]["value"] == 1.0 and left_only["right"] is None
    # Both sides uncovered counts as agreement, keeping the partition contiguous.
    assert both_uncovered["left"] is None and both_uncovered["right"] is None
    assert right_only["left"] is None and right_only["right"]["value"] == 2.0


def test_compare_mixed_intervals_sorted_and_not_overmerged(client):
    # L: [0,10)=1.0 [10,20)=2.0 ; R: [0,5)=1.0 [5,15)=withdrawn [15,25)=2.0
    _post(client, _event("v1", "ROOT", 0, 20, value=1.0, batch="L"), batch="L")
    _post(client, _event("v2", "v1", 10, 20, value=2.0, batch="L"), batch="L")
    _post(client, _event("w1", "ROOT", 0, 5, value=1.0, batch="R"), batch="R")
    _post(
        client,
        _event("w2", "w1", 5, 15, withdrawn=True, value=None, batch="R"),
        batch="R",
    )
    _post(client, _event("w3", "w2", 15, 25, value=2.0, batch="R"), batch="R")
    body = _compare(client).json()
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 5, "equal"),
        (5, 10, "state_mismatch"),
        # Same kind as the previous interval but different left payload: no merge.
        (10, 15, "state_mismatch"),
        (15, 20, "equal"),
        (20, 25, "right_only"),
    ]


def test_compare_incomplete_reports_gaps(client):
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0, batch="L"), batch="L")
    _post(client, _event("v1", "ROOT", 0, 100, value=1.0, batch="R"), batch="R")
    _post(client, _event("v2", "missing", 0, 50, value=9.0, batch="R"), batch="R")
    body = _compare(client).json()
    assert body["incomplete"] is True
    [reason] = body["incomplete_reasons"]
    assert reason["side"] == "right"
    assert reason["batch_id"] == "R"
    assert reason["reason"] == "gaps"
    assert reason["affected_revisions"] == ["v2"]
    assert reason["missing_revision_ids"] == ["missing"]
    # The diff itself is still computed from the grounded folds.
    assert [i["kind"] for i in body["intervals"]] == ["equal"]


def test_compare_incomplete_reports_invalid_cycle(client):
    _post(client, _event("c1", "c2", 0, 10, value=1.0, batch="L", key="c1"), batch="L")
    _post(client, _event("c2", "c1", 0, 10, value=2.0, batch="L", key="c2"), batch="L")
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="R"), batch="R")
    body = _compare(client).json()
    assert body["incomplete"] is True
    [reason] = body["incomplete_reasons"]
    assert reason["side"] == "left"
    assert reason["reason"] == "invalid_revisions"
    assert reason["affected_revisions"] == ["c1", "c2"]
    assert reason["missing_revision_ids"] == []
    # The left fold is empty (cycle excluded), so coverage is right-only.
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 10, "right_only")
    ]


def test_compare_deterministic_regardless_of_arrival_order(client):
    left_events = [
        _event("r", "ROOT", 0, 100, value=1.0, batch="L", key="lr"),
        _event("a", "r", 0, 50, value=2.0, batch="L", key="la"),
        _event("b", "r", 25, 75, value=3.0, batch="L", key="lb"),
    ]
    right_events = [
        _event("r", "ROOT", 0, 100, value=1.0, batch="R", key="rr"),
        _event("a", "r", 0, 50, value=2.0, batch="R", key="ra"),
        _event("b", "r", 25, 75, value=3.0, batch="R", key="rb"),
    ]
    for e in left_events:
        _post(client, e, batch="L")
    for e in reversed(right_events):
        _post(client, e, batch="R")
    body = _compare(client).json()
    assert body["left_segments_hash"] == body["right_segments_hash"]
    assert {i["kind"] for i in body["intervals"]} == {"equal"}


def test_compare_batch_with_itself_is_all_equal(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="L"), batch="L")
    body = _compare(client, left="L", right="L").json()
    assert [i["kind"] for i in body["intervals"]] == ["equal"]
    assert body["incomplete"] is False


def test_compare_unknown_batch_or_sensor_404(client):
    assert _compare(client).status_code == 404
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="L"), batch="L")
    # Right batch does not exist yet.
    r = _compare(client)
    assert r.status_code == 404
    assert "'R'" in r.json()["detail"]
    # Sensor S1 unknown in the right batch (only SX there).
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="R", sensor="SX"), batch="R")
    r = _compare(client)
    assert r.status_code == 404
    assert "S1" in r.json()["detail"] and "'R'" in r.json()["detail"]
    # Sensor SX unknown in the left batch.
    r = _compare(client, sensor="SX")
    assert r.status_code == 404
    assert "SX" in r.json()["detail"] and "'L'" in r.json()["detail"]


def test_compare_invalid_params_422(client):
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="L"), batch="L")
    _post(client, _event("v1", "ROOT", 0, 10, value=1.0, batch="R"), batch="R")
    # Missing query parameters.
    assert client.get("/api/v1/batches/compare").status_code == 422
    # Empty identifiers.
    assert _compare(client, left="").status_code == 422
    assert _compare(client, right="").status_code == 422
    assert _compare(client, sensor="").status_code == 422
