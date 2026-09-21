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


def _compare(client, left="BL", right="BR", sensor="S1"):
    return client.get(
        "/api/v1/batches/compare",
        params={"left_batch_id": left, "right_batch_id": right, "sensor_id": sensor},
    )


def _post_to(client, event, batch):
    """Post an event whose body batch_id is aligned with the path batch."""
    return _post(client, {**event, "batch_id": batch}, batch=batch)


def test_compare_identical_timelines_are_all_equal(client):
    _post_to(client, _event("v1", "ROOT", 0, 100, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("v2", "v1", 10, 20, value=2.0, key="kl2"), "BL")
    # Different revision ids/history, same observable timeline.
    _post_to(client, _event("w1", "ROOT", 0, 100, value=1.0, key="kr1"), "BR")
    _post_to(client, _event("w2", "w1", 10, 20, value=2.0, key="kr2"), "BR")

    r = _compare(client)
    assert r.status_code == 200
    body = r.json()
    assert body["left_batch_id"] == "BL" and body["right_batch_id"] == "BR"
    assert body["sensor_id"] == "S1"
    assert body["differs"] is False
    assert body["incomplete"] is False
    assert body["incomplete_reasons"] == []
    assert [i["kind"] for i in body["intervals"]] == ["equal"] * 3
    assert [(i["start_ns"], i["end_ns"]) for i in body["intervals"]] == [
        (0, 10),
        (10, 20),
        (20, 100),
    ]
    mid = body["intervals"][1]
    assert mid["left"] == {"state": "calibrated", "value": 2.0, "unit": "mm"}
    assert mid["right"] == {"state": "calibrated", "value": 2.0, "unit": "mm"}
    # Provenance differs, so the exact-identity hashes differ too.
    assert body["left_segments_hash"] != body["right_segments_hash"]


def test_compare_merges_adjacent_equal_across_revision_boundaries(client):
    # Left keeps a provenance boundary at 10 (v1 -> v2, same value); the diff
    # must merge the two equal spans into one interval.
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("v2", "v1", 10, 20, value=1.0, key="kl2"), "BL")
    _post_to(client, _event("w1", "ROOT", 0, 20, value=1.0, key="kr1"), "BR")

    body = _compare(client).json()
    assert body["intervals"] == [
        {
            "start_ns": 0,
            "end_ns": 20,
            "kind": "equal",
            "left": {"state": "calibrated", "value": 1.0, "unit": "mm"},
            "right": {"state": "calibrated", "value": 1.0, "unit": "mm"},
        }
    ]
    assert body["differs"] is False


def test_compare_left_only_right_only_and_half_open_boundaries(client):
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("w1", "ROOT", 10, 20, value=1.0, key="kr1"), "BR")

    body = _compare(client).json()
    assert body["differs"] is True
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 10, "left_only"),
        (10, 20, "right_only"),
    ]
    first, second = body["intervals"]
    assert first["left"] == {"state": "calibrated", "value": 1.0, "unit": "mm"}
    assert first["right"] is None
    assert second["left"] is None
    assert second["right"] == {"state": "calibrated", "value": 1.0, "unit": "mm"}


def test_compare_value_mismatch_on_value_and_unit(client):
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("v2", "v1", 10, 20, value=2.0, key="kl2"), "BL")
    # Same values but a different unit on [0,10); different value on [10,20).
    _post_to(
        client,
        _event("w1", "ROOT", 0, 10, value=1.0, unit="cm", key="kr1"),
        "BR",
    )
    _post_to(client, _event("w2", "w1", 10, 20, value=3.0, key="kr2"), "BR")

    body = _compare(client).json()
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 10, "value_mismatch"),
        (10, 20, "value_mismatch"),
    ]
    assert body["intervals"][0]["left"]["unit"] == "mm"
    assert body["intervals"][0]["right"]["unit"] == "cm"
    assert body["intervals"][1]["left"]["value"] == 2.0
    assert body["intervals"][1]["right"]["value"] == 3.0


def test_compare_state_mismatch_calibrated_vs_withdrawn(client):
    _post_to(client, _event("v1", "ROOT", 0, 100, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("w1", "ROOT", 0, 100, value=1.0, key="kr1"), "BR")
    _post_to(
        client,
        _event("w2", "w1", 30, 70, withdrawn=True, value=None, key="kr2"),
        "BR",
    )

    body = _compare(client).json()
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 30, "equal"),
        (30, 70, "state_mismatch"),
        (70, 100, "equal"),
    ]
    mid = body["intervals"][1]
    assert mid["left"] == {"state": "calibrated", "value": 1.0, "unit": "mm"}
    assert mid["right"] == {"state": "withdrawn", "value": None, "unit": None}


def test_compare_withdrawn_on_both_sides_is_equal(client):
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(
        client,
        _event("v2", "v1", 0, 10, withdrawn=True, value=None, key="kl2"),
        "BL",
    )
    _post_to(client, _event("w1", "ROOT", 0, 10, value=9.0, key="kr1"), "BR")
    _post_to(
        client,
        _event("w2", "w1", 0, 10, withdrawn=True, value=None, key="kr2"),
        "BR",
    )

    body = _compare(client).json()
    assert [(i["kind"], i["left"]["state"], i["right"]["state"])
            for i in body["intervals"]] == [("equal", "withdrawn", "withdrawn")]
    assert body["differs"] is False


def test_compare_uncovered_hole_on_both_sides_is_equal(client):
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("v2", "v1", 20, 30, value=2.0, key="kl2"), "BL")
    _post_to(client, _event("w1", "ROOT", 0, 10, value=1.0, key="kr1"), "BR")
    _post_to(client, _event("w2", "w1", 20, 30, value=2.0, key="kr2"), "BR")

    body = _compare(client).json()
    assert [(i["start_ns"], i["end_ns"], i["kind"]) for i in body["intervals"]] == [
        (0, 10, "equal"),
        (10, 20, "equal"),
        (20, 30, "equal"),
    ]
    hole = body["intervals"][1]
    assert hole["left"] is None and hole["right"] is None
    assert body["differs"] is False


def test_compare_incomplete_due_to_gaps_and_invalid(client):
    # Left: child waiting on a missing parent (backfillable gap).
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("v3", "v2", 0, 10, value=3.0, key="kl3"), "BL")
    # Right: a parent cycle (invalid revisions).
    _post_to(client, _event("w1", "ROOT", 0, 10, value=1.0, key="kr1"), "BR")
    _post_to(client, _event("c1", "c2", 0, 5, value=1.0, key="kc1"), "BR")
    _post_to(client, _event("c2", "c1", 0, 5, value=1.0, key="kc2"), "BR")

    body = _compare(client).json()
    assert body["incomplete"] is True
    reasons = {(r["side"], r["reason"]): r for r in body["incomplete_reasons"]}
    gap = reasons[("left", "gaps")]
    assert gap["batch_id"] == "BL"
    assert gap["missing_revision_ids"] == ["v2"]
    assert gap["awaited_by"] == ["v3"]
    inv = reasons[("right", "invalid_revisions")]
    assert inv["batch_id"] == "BR"
    assert inv["invalid_revision_ids"] == ["c1", "c2"]
    # The diff itself is still computed from the grounded folds.
    assert body["intervals"] == [
        {
            "start_ns": 0,
            "end_ns": 10,
            "kind": "equal",
            "left": {"state": "calibrated", "value": 1.0, "unit": "mm"},
            "right": {"state": "calibrated", "value": 1.0, "unit": "mm"},
        }
    ]


def test_compare_incomplete_resolves_after_backfill(client):
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    _post_to(client, _event("v2", "ghost", 0, 10, value=2.0, key="kl2"), "BL")
    _post_to(client, _event("w1", "ROOT", 0, 10, value=2.0, key="kr1"), "BR")

    body = _compare(client).json()
    assert body["incomplete"] is True
    assert body["intervals"][0]["kind"] == "value_mismatch"

    _post_to(client, _event("ghost", "ROOT", 0, 10, value=9.0, key="kl0"), "BL")
    body = _compare(client).json()
    assert body["incomplete"] is False
    assert body["incomplete_reasons"] == []
    # Backfilled chain: v2 (depth 1) now overwrites ghost on [0,10).
    assert body["intervals"][0]["kind"] == "equal"


def test_compare_unknown_batch_or_sensor_404(client):
    assert _compare(client, left="NOPE").status_code == 404
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    assert _compare(client, right="NOPE").status_code == 404
    # Right batch does not exist yet.
    assert _compare(client).status_code == 404
    _post_to(client, _event("w1", "ROOT", 0, 10, value=1.0, key="kr1"), "BR")
    assert _compare(client, sensor="SX").status_code == 404
    assert _compare(client).status_code == 200


def test_compare_rejects_invalid_params(client):
    _post_to(client, _event("v1", "ROOT", 0, 10, value=1.0, key="kl1"), "BL")
    base = "/api/v1/batches/compare"
    # Missing query parameters.
    assert client.get(base).status_code == 422
    assert client.get(base, params={"left_batch_id": "BL"}).status_code == 422
    # Empty parameter values.
    assert _compare(client, left="").status_code == 422
    assert _compare(client, sensor="").status_code == 422


def test_compare_is_deterministic_across_arrival_orders(client):
    def submit(batch, events):
        for e in events:
            resp = _post_to(client, e, batch)
            assert resp.status_code == 200

    left_events = [
        _event("r", "ROOT", 0, 100, value=1.0, key="kl0"),
        _event("a", "r", 0, 50, value=2.0, key="kl1"),
        _event("b", "r", 25, 75, withdrawn=True, value=None, key="kl2"),
    ]
    right_events = [
        _event("x", "ROOT", 0, 100, value=1.0, key="kr0"),
        _event("y", "x", 0, 25, value=2.0, key="kr1"),
        _event("z", "x", 50, 75, value=3.0, key="kr2"),
    ]
    submit("BL", left_events)
    submit("BR", right_events)
    first = _compare(client).json()

    client.post("/admin/reset")
    submit("BL", list(reversed(left_events)))
    submit("BR", list(reversed(right_events)))
    assert _compare(client).json() == first
