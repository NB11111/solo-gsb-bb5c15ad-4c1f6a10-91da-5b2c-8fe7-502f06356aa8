"""Deterministic effective-calibration-timeline engine.

The engine is pure: given a :class:`~app.storage.BatchStore` snapshot it
produces per-sensor timelines, backfillable gaps and content hashes, without
keeping state of its own.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from .models import ROOT_PARENT
from .storage import BatchStore, StoredEvent
# Revision states (reachable from a known root vs. waiting on a missing
# ancestor vs. structurally unusable, e.g. part of a parent cycle).
GROUNDED = "grounded"
BLOCKED = "blocked"
INVALID = "invalid"

# Batch-comparison interval kinds (observable outcome of the half-open diff
# scan at every instant of the union of both covered ranges).
DIFF_EQUAL = "equal"
DIFF_LEFT_ONLY = "left_only"
DIFF_RIGHT_ONLY = "right_only"
DIFF_VALUE_MISMATCH = "value_mismatch"
DIFF_STATE_MISMATCH = "state_mismatch"


@dataclass(frozen=True)
class Segment:
    start_ns: int
    end_ns: int
    state: str  # "calibrated" | "withdrawn"
    value: Optional[float]
    unit: Optional[str]
    revision_id: str


# ---------------------------------------------------------------------------
# Causal classification
# ---------------------------------------------------------------------------


def classify_sensor(
    events: dict[str, StoredEvent],
) -> tuple[dict[str, str], dict[str, int]]:
    """Classify every revision of one sensor and compute its causal depth.

    Depth is the number of parent hops from a ``ROOT`` revision (roots are
    depth 0). Returns ``(states, depths)``.

    States:

    * ``grounded`` - reachable from a known ``ROOT`` revision;
    * ``blocked``  - has an unknown (not yet uploaded) ancestor;
    * ``invalid``  - participates in a parent cycle or descends from one.
    """
    states: dict[str, str] = {}
    depths: dict[str, int] = {}

    def resolve(node: str) -> None:
        """Iterative postorder DFS rooted at ``node``."""
        # Frames: (node, entered). `entered` frames have already pushed their
        # parent frame and just need post-order finalization.
        stack: list[tuple[str, bool]] = [(node, False)]
        on_path: set[str] = set()
        while stack:
            cur, entered = stack.pop()
            if cur in states:
                continue
            parent = events[cur].parent_id

            if not entered:
                if cur in on_path:  # pragma: no cover - pushed once normally
                    continue
                on_path.add(cur)
                if parent == ROOT_PARENT:
                    states[cur] = GROUNDED
                    depths[cur] = 0
                    on_path.discard(cur)
                    continue
                if parent not in events:
                    states[cur] = BLOCKED
                    on_path.discard(cur)
                    continue
                if parent in states:
                    _inherit(cur, parent, states, depths)
                    on_path.discard(cur)
                    continue
                # Resolve parent first, then finalize this node.
                stack.append((cur, True))
                stack.append((parent, False))
            else:
                if parent in on_path and parent not in states:
                    # Back edge: the parent chain currently on the stack is a
                    # cycle. Mark every still-open node invalid; finalization
                    # frames will then propagate INVALID to descendants.
                    for n in on_path:
                        states[n] = INVALID
                elif parent in states:
                    _inherit(cur, parent, states, depths)
                else:  # pragma: no cover - defensive
                    states[cur] = INVALID
                on_path.discard(cur)

    for rev in sorted(events):
        if rev not in states:
            resolve(rev)

    return states, depths


def _inherit(
    node: str,
    parent: str,
    states: dict[str, str],
    depths: dict[str, int],
) -> None:
    pstate = states[parent]
    if pstate == GROUNDED:
        states[node] = GROUNDED
        depths[node] = depths[parent] + 1
    elif pstate == INVALID:
        states[node] = INVALID
    else:
        states[node] = BLOCKED


# ---------------------------------------------------------------------------
# Interval folding
# ---------------------------------------------------------------------------


def _apply(segments: list[Segment], event: StoredEvent) -> None:
    """Overwrite the half-open window ``[start_ns, end_ns)`` in ``segments``.

    Existing segments are split at the window boundaries; the window itself is
    replaced by one segment describing the applying revision.
    """
    state = "withdrawn" if event.withdrawn else "calibrated"
    new_seg = Segment(
        start_ns=event.start_ns,
        end_ns=event.end_ns,
        state=state,
        value=None if event.withdrawn else float(event.value),
        unit=None if event.withdrawn else event.unit,
        revision_id=event.revision_id,
    )

    result: list[Segment] = []
    lo, hi = event.start_ns, event.end_ns
    for seg in segments:
        if seg.end_ns <= lo or seg.start_ns >= hi:
            result.append(seg)
            continue
        # Overlap: keep the non-covered parts (half-open intervals).
        if seg.start_ns < lo:
            result.append(
                Segment(seg.start_ns, lo, seg.state, seg.value, seg.unit, seg.revision_id)
            )
        if seg.end_ns > hi:
            result.append(
                Segment(hi, seg.end_ns, seg.state, seg.value, seg.unit, seg.revision_id)
            )
    # Insert the replacement and re-sort; windows may bridge old segments.
    result.append(new_seg)
    result.sort(key=lambda s: (s.start_ns, s.end_ns))

    # Merge adjacent segments that are identical in every output attribute
    # (different revision provenance keeps a boundary).
    merged: list[Segment] = []
    for seg in result:
        if merged and merged[-1].end_ns == seg.start_ns and _same_payload(
            merged[-1], seg
        ):
            merged[-1] = Segment(
                merged[-1].start_ns,
                seg.end_ns,
                seg.state,
                seg.value,
                seg.unit,
                seg.revision_id,
            )
        else:
            merged.append(seg)
    segments[:] = merged


def _same_payload(a: Segment, b: Segment) -> bool:
    return (
        a.state == b.state
        and a.revision_id == b.revision_id
        and a.value == b.value
        and a.unit == b.unit
    )


def build_timeline(events: list[StoredEvent], depths: dict[str, int]) -> list[Segment]:
    """Fold grounded events into the effective timeline.

    Deterministic application order: causal depth first (descendants overwrite
    ancestors), then ascending ``revision_id`` to break ties between
    incomparable causal branches; the larger id is therefore applied last and
    wins on same-depth overlaps.
    """
    ordered = sorted(events, key=lambda e: (depths[e.revision_id], e.revision_id))
    segments: list[Segment] = []
    for event in ordered:
        _apply(segments, event)
    return segments


# ---------------------------------------------------------------------------
# Gaps
# ---------------------------------------------------------------------------


def collect_gaps(
    batch_id: str,
    sensor_events: dict[str, StoredEvent],
    states: dict[str, str],
) -> list[dict]:
    """One entry per distinct missing parent, listing every awaiting child."""
    gaps = []
    sensor_id = next(iter(sensor_events.values())).sensor_id
    awaited: dict[str, set[str]] = defaultdict(set)
    referenced_from: dict[str, str] = {}
    for event in sensor_events.values():
        if states.get(event.revision_id) != BLOCKED:
            continue
        parent = event.parent_id
        if parent != ROOT_PARENT and parent not in sensor_events:
            awaited[parent].add(event.revision_id)
            # Deterministic "first referrer" = lexicographically smallest child.
            if parent not in referenced_from or event.revision_id < referenced_from[parent]:
                referenced_from[parent] = event.revision_id

    for missing in sorted(awaited):
        gaps.append(
            {
                "batch_id": batch_id,
                "sensor_id": sensor_id,
                "missing_revision_id": missing,
                "awaited_by": sorted(awaited[missing]),
                "referenced_from": referenced_from[missing],
            }
        )
    return gaps


# ---------------------------------------------------------------------------
# Batch comparison (half-open interval diff scan)
# ---------------------------------------------------------------------------


def _side_payload(seg: Optional[Segment]) -> Optional[dict]:
    """Observable state of one side; ``None`` when that side does not cover."""
    if seg is None:
        return None
    return {"state": seg.state, "value": seg.value, "unit": seg.unit}


def _diff_kind(left: Optional[Segment], right: Optional[Segment]) -> str:
    """Classify one instant given the segments covering it on each side.

    Provenance (``revision_id``) is deliberately not compared: two batches
    that converged to the same observable timeline via different revision
    histories are ``equal``. Spans covered by neither side are ``equal``
    with both payloads null.
    """
    if left is None:
        return DIFF_EQUAL if right is None else DIFF_RIGHT_ONLY
    if right is None:
        return DIFF_LEFT_ONLY
    if left.state != right.state:
        return DIFF_STATE_MISMATCH
    if left.state == "withdrawn":
        return DIFF_EQUAL  # tombstones carry no value payload
    return (
        DIFF_EQUAL
        if left.value == right.value and left.unit == right.unit
        else DIFF_VALUE_MISMATCH
    )


def diff_timelines(left: list[Segment], right: list[Segment]) -> list[dict]:
    """Diff two folded timelines into time-sorted, merged half-open intervals.

    Every atomic span between two consecutive segment boundaries of either
    timeline is classified (see :func:`_diff_kind`); adjacent spans with the
    same kind and identical observable payloads on both sides are merged, so
    provenance-only boundaries inside one timeline do not fragment the diff.
    """
    bounds = sorted(
        {s.start_ns for s in left}
        | {s.end_ns for s in left}
        | {s.start_ns for s in right}
        | {s.end_ns for s in right}
    )
    intervals: list[dict] = []
    li = ri = 0
    for lo, hi in zip(bounds, bounds[1:]):
        while li < len(left) and left[li].end_ns <= lo:
            li += 1
        while ri < len(right) and right[ri].end_ns <= lo:
            ri += 1
        lseg = left[li] if li < len(left) and left[li].start_ns <= lo else None
        rseg = right[ri] if ri < len(right) and right[ri].start_ns <= lo else None
        kind = _diff_kind(lseg, rseg)
        lp = _side_payload(lseg)
        rp = _side_payload(rseg)
        if (
            intervals
            and intervals[-1]["end_ns"] == lo
            and intervals[-1]["kind"] == kind
            and intervals[-1]["left"] == lp
            and intervals[-1]["right"] == rp
        ):
            intervals[-1]["end_ns"] = hi
        else:
            intervals.append(
                {"start_ns": lo, "end_ns": hi, "kind": kind, "left": lp, "right": rp}
            )
    return intervals


# ---------------------------------------------------------------------------
# Hashing / serialization
# ---------------------------------------------------------------------------


def segment_to_dict(seg: Segment) -> dict:
    return {
        "start_ns": seg.start_ns,
        "end_ns": seg.end_ns,
        "state": seg.state,
        "value": None if seg.value is None else float(seg.value),
        "unit": seg.unit,
        "revision_id": seg.revision_id,
    }


def hash_segments(segments: list[Segment]) -> str:
    """Stable SHA-256 over the canonical segment list."""
    payload = json.dumps(
        [segment_to_dict(s) for s in segments],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_batch(sensor_payloads: list[dict]) -> str:
    payload = json.dumps(
        sensor_payloads, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Batch-level orchestration
# ---------------------------------------------------------------------------


def group_by_sensor(store: BatchStore) -> dict[str, dict[str, StoredEvent]]:
    """Group every stored revision of a batch by its owning sensor."""
    by_sensor: dict[str, dict[str, StoredEvent]] = {}
    for event in store.events.values():
        by_sensor.setdefault(event.sensor_id, {})[event.revision_id] = event
    return by_sensor


def sensor_known(store: BatchStore, sensor_id: str) -> bool:
    """True when the batch holds at least one revision of ``sensor_id``."""
    return any(event.sensor_id == sensor_id for event in store.events.values())


def _parent_chain(revision_id: str, events: dict[str, StoredEvent]) -> list[str]:
    """Ordered chain from ``revision_id`` to ``ROOT`` inclusive.

    The revision itself is first and the literal ``"ROOT"`` is last. Only
    called for grounded revisions, so every parent hop is known.
    """
    chain = [revision_id]
    parent = events[revision_id].parent_id
    while parent != ROOT_PARENT:
        chain.append(parent)
        parent = events[parent].parent_id
    chain.append(ROOT_PARENT)
    return chain


def query_sample(
    sensor_id: str,
    timestamp_ns: int,
    sensor_events: dict[str, StoredEvent],
    states: dict[str, str],
    depths: dict[str, int],
) -> dict:
    """Resolve one sample instant of one sensor.

    Blocking takes precedence: if any known-but-not-grounded revision covers
    the instant, backfilling its missing ancestors could change which revision
    wins the deterministic fold at that point, so the point is reported as
    ``blocked`` with the distinct missing ancestors that must be uploaded.
    Otherwise the instant is looked up in the folded effective timeline,
    reusing the exact same fold order as the full batch result.
    """
    covering_blocked = sorted(
        rev
        for rev, event in sensor_events.items()
        if states[rev] == BLOCKED
        and event.start_ns <= timestamp_ns < event.end_ns
    )
    if covering_blocked:
        missing: set[str] = set()
        for rev in covering_blocked:
            parent = sensor_events[rev].parent_id
            while parent != ROOT_PARENT and parent in sensor_events:
                parent = sensor_events[parent].parent_id
            if parent != ROOT_PARENT:
                missing.add(parent)
        return {
            "sensor_id": sensor_id,
            "timestamp_ns": timestamp_ns,
            "state": "blocked",
            "value": None,
            "unit": None,
            "revision_id": None,
            "parent_chain": [],
            "missing_ancestors": sorted(missing),
        }

    grounded_events = [
        e for e in sensor_events.values() if states[e.revision_id] == GROUNDED
    ]
    segments = build_timeline(grounded_events, depths)
    for seg in segments:
        if seg.start_ns <= timestamp_ns < seg.end_ns:
            return {
                "sensor_id": sensor_id,
                "timestamp_ns": timestamp_ns,
                "state": seg.state,
                "value": seg.value,
                "unit": seg.unit,
                "revision_id": seg.revision_id,
                "parent_chain": _parent_chain(seg.revision_id, sensor_events),
                "missing_ancestors": [],
            }

    return {
        "sensor_id": sensor_id,
        "timestamp_ns": timestamp_ns,
        "state": "uncovered",
        "value": None,
        "unit": None,
        "revision_id": None,
        "parent_chain": [],
        "missing_ancestors": [],
    }


def evaluate_sample(
    batch_id: str, sensor_id: str, timestamp_ns: int, store: BatchStore
) -> Optional[dict]:
    """Compute the sample-point document; ``None`` when the sensor is unknown."""
    by_sensor = group_by_sensor(store)
    sensor_events = by_sensor.get(sensor_id)
    if sensor_events is None:
        return None
    states, depths = classify_sensor(sensor_events)
    doc = query_sample(sensor_id, timestamp_ns, sensor_events, states, depths)
    doc = {"batch_id": batch_id, **doc}
    return doc


def evaluate_batch(batch_id: str, store: BatchStore) -> dict:
    """Compute the complete, deterministic result document for one batch."""
    by_sensor = group_by_sensor(store)

    sensors_out: list[dict] = []
    gaps_out: list[dict] = []
    totals = {GROUNDED: 0, BLOCKED: 0, INVALID: 0}

    for sensor_id in sorted(by_sensor):
        sensor_events = by_sensor[sensor_id]
        states, depths = classify_sensor(sensor_events)
        gaps_out.extend(collect_gaps(batch_id, sensor_events, states))

        for state in states.values():
            totals[state] += 1

        grounded_events = [
            e for e in sensor_events.values() if states[e.revision_id] == GROUNDED
        ]
        segments = build_timeline(grounded_events, depths)
        seg_dicts = [segment_to_dict(s) for s in segments]
        sensor_doc = {
            "sensor_id": sensor_id,
            "grounded": not any(
                states[r] != GROUNDED for r in sensor_events
            ),
            "segments": seg_dicts,
            "segments_hash": hash_segments(segments),
            "covered_start_ns": segments[0].start_ns if segments else None,
            "covered_end_ns": segments[-1].end_ns if segments else None,
        }
        sensors_out.append(sensor_doc)

    gaps_out.sort(key=lambda g: (g["sensor_id"], g["missing_revision_id"]))
    timeline_hash = hash_batch(
        [{"sensor_id": s["sensor_id"], "segments": s["segments"]} for s in sensors_out]
    )

    revisions_total = sum(totals.values())
    return {
        "batch_id": batch_id,
        "sensors": sensors_out,
        "gaps": gaps_out,
        "revisions_total": revisions_total,
        "revisions_grounded": totals[GROUNDED],
        "revisions_blocked": totals[BLOCKED],
        "revisions_invalid": totals[INVALID],
        "complete": not gaps_out and totals[INVALID] == 0 and revisions_total > 0,
        "timeline_hash": timeline_hash,
    }


# ---------------------------------------------------------------------------
# Batch comparison orchestration
# ---------------------------------------------------------------------------


def _sensor_fold(
    batch_id: str, sensor_id: str, store: BatchStore
) -> tuple[list[Segment], list[dict], list[str]]:
    """Fold one sensor of one batch: ``(segments, gaps, invalid_revision_ids)``.

    Precondition: the sensor is present in ``store`` (see :func:`sensor_known`).
    """
    sensor_events = group_by_sensor(store)[sensor_id]
    states, depths = classify_sensor(sensor_events)
    grounded_events = [
        e for e in sensor_events.values() if states[e.revision_id] == GROUNDED
    ]
    segments = build_timeline(grounded_events, depths)
    gaps = collect_gaps(batch_id, sensor_events, states)
    invalid = sorted(rev for rev, state in states.items() if state == INVALID)
    return segments, gaps, invalid


def evaluate_compare(
    left_batch_id: str,
    right_batch_id: str,
    sensor_id: str,
    left_store: BatchStore,
    right_store: BatchStore,
) -> dict:
    """Compute the deterministic comparison document for one sensor across two
    batches.

    The diff reuses each side's folded effective timeline as-is; pending
    backfills (gaps) or cyclic (invalid) revisions on either side are reported
    as ``incomplete_reasons`` because resolving them could still change the
    corresponding timeline. Precondition: ``sensor_id`` is known in both
    stores (see :func:`sensor_known`).
    """
    left_segments, left_gaps, left_invalid = _sensor_fold(
        left_batch_id, sensor_id, left_store
    )
    right_segments, right_gaps, right_invalid = _sensor_fold(
        right_batch_id, sensor_id, right_store
    )

    intervals = diff_timelines(left_segments, right_segments)

    reasons: list[dict] = []
    for side, batch_id, gaps, invalid in (
        ("left", left_batch_id, left_gaps, left_invalid),
        ("right", right_batch_id, right_gaps, right_invalid),
    ):
        if gaps:
            reasons.append(
                {
                    "side": side,
                    "batch_id": batch_id,
                    "reason": "gaps",
                    "missing_revision_ids": [g["missing_revision_id"] for g in gaps],
                    "awaited_by": sorted({c for g in gaps for c in g["awaited_by"]}),
                    "invalid_revision_ids": [],
                }
            )
        if invalid:
            reasons.append(
                {
                    "side": side,
                    "batch_id": batch_id,
                    "reason": "invalid_revisions",
                    "missing_revision_ids": [],
                    "awaited_by": [],
                    "invalid_revision_ids": invalid,
                }
            )

    return {
        "left_batch_id": left_batch_id,
        "right_batch_id": right_batch_id,
        "sensor_id": sensor_id,
        "intervals": intervals,
        "differs": any(i["kind"] != DIFF_EQUAL for i in intervals),
        "incomplete": bool(reasons),
        "incomplete_reasons": reasons,
        "left_segments_hash": hash_segments(left_segments),
        "right_segments_hash": hash_segments(right_segments),
    }
