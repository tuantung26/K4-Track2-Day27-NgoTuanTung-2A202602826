"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.
"""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _has_error(result):
    return "error" in result


def check_data_batch(payload, ctx):
    result = ctx.tools.batch_profile(payload["batch_id"])
    if _has_error(result):
        return Verdict(alert=False, reason=result["error"], pillar="checks")

    baseline = ctx.baseline
    anomalies = []
    row_margin = (baseline["row_count_max"] - baseline["row_count_min"]) * 0.1
    row_low = result["row_count"] < baseline["row_count_min"] + row_margin
    row_high = result["row_count"] > baseline["row_count_max"] - row_margin
    if not (
        baseline["row_count_min"]
        <= result["row_count"]
        <= baseline["row_count_max"]
    ):
        anomalies.append("row_count")
    null_rate = result["null_rate"]["customer_id"]
    null_high = null_rate > baseline["null_rate_max"] * 0.9
    if null_rate > baseline["null_rate_max"]:
        anomalies.append("customer_id_null_rate")
    amount_margin = (baseline["mean_amount_max"] - baseline["mean_amount_min"]) * 0.1
    amount_low = result["mean_amount"] < baseline["mean_amount_min"] + amount_margin
    amount_high = result["mean_amount"] > baseline["mean_amount_max"] - amount_margin
    if not (
        baseline["mean_amount_min"]
        <= result["mean_amount"]
        <= baseline["mean_amount_max"]
    ):
        anomalies.append("mean_amount")
    stale_high = result["staleness_min"] > baseline["staleness_min_max"] * 0.9
    if result["staleness_min"] > baseline["staleness_min_max"]:
        anomalies.append("staleness")
    if sum((row_low, row_high, null_high, amount_low, amount_high, stale_high)) >= 2:
        anomalies.append("combined marginal shifts")

    return Verdict(
        alert=bool(anomalies),
        confidence=1.0 if anomalies else 0.0,
        reason="out-of-baseline: " + ", ".join(anomalies) if anomalies else "",
        pillar="checks",
    )


def check_contract_checkpoint(payload, ctx):
    result = ctx.tools.contract_diff(
        payload["contract_id"], payload["checkpoint_batch_id"]
    )
    if _has_error(result):
        return Verdict(alert=False, reason=result["error"], pillar="contracts")

    anomalies = list(result["violations"])
    if result["freshness_delay_min"] > (
        ctx.baseline["freshness_delay_max_min"] * 0.9
    ):
        anomalies.append("freshness_delay")
    return Verdict(
        alert=bool(anomalies),
        confidence=1.0 if anomalies else 0.0,
        reason="contract violation: " + ", ".join(anomalies) if anomalies else "",
        pillar="contracts",
    )


def check_lineage_run(payload, ctx):
    result = ctx.tools.lineage_graph_slice(payload["run_id"])
    if _has_error(result):
        return Verdict(alert=False, reason=result["error"], pillar="lineage")

    anomalies = []
    if result["duration_ms"] > ctx.baseline["lineage_duration_ms_max"]:
        anomalies.append("duration")
    upstream = result["actual_upstream"]
    if not upstream:
        anomalies.append("missing_upstream")
    elif _has_missing_upstream_edge(payload, upstream, ctx):
        anomalies.append("missing_upstream_edge")
    if result["actual_downstream_count"] <= 0:
        anomalies.append("orphaned_output")
    return Verdict(
        alert=bool(anomalies),
        confidence=1.0 if anomalies else 0.0,
        reason="lineage anomaly: " + ", ".join(anomalies) if anomalies else "",
        pillar="lineage",
    )


def _has_missing_upstream_edge(payload, upstream, ctx):
    if not isinstance(upstream, (list, tuple, set)):
        return False

    identity_fields = ("pipeline", "transform", "node", "table", "asset", "model", "task")
    identity = tuple(
        sorted(
            (key, str(value))
            for key, value in payload.items()
            if any(field in key.lower() for field in identity_fields)
            and isinstance(value, (str, int, float, bool))
        )
    )
    topology = frozenset(map(str, upstream))
    histories = ctx.state.setdefault("lineage_topologies", {})
    known = histories.setdefault(identity, {})

    incomplete = any(
        count >= 2 and topology < previous
        for previous, count in known.items()
    )
    if not incomplete:
        known[topology] = known.get(topology, 0) + 1
    return incomplete


def check_feature_materialization(payload, ctx):
    result = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if _has_error(result):
        return Verdict(alert=False, reason=result["error"], pillar="ai_infra")

    shifted = (
        result["mean_shift_sigma"]
        > ctx.baseline["feature_mean_shift_sigma_max"]
    )
    return Verdict(
        alert=shifted,
        confidence=1.0 if shifted else 0.0,
        reason="feature mean shift exceeds baseline" if shifted else "",
        pillar="ai_infra",
    )


def check_embedding_batch(payload, ctx):
    result = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if _has_error(result):
        return Verdict(alert=False, reason=result["error"], pillar="ai_infra")

    anomalies = []
    centroid_high = result["centroid_shift"] > (
        ctx.baseline["embedding_centroid_shift_max"] * 0.9
    )
    age_high = result["avg_doc_age_days"] > (
        ctx.baseline["corpus_avg_doc_age_days_max"] * 0.9
    )
    if result["centroid_shift"] > ctx.baseline["embedding_centroid_shift_max"]:
        anomalies.append("centroid_shift")
    if result["avg_doc_age_days"] > ctx.baseline["corpus_avg_doc_age_days_max"]:
        anomalies.append("document_age")
    if centroid_high and age_high and not anomalies:
        anomalies.append("combined marginal shifts")
    return Verdict(
        alert=bool(anomalies),
        confidence=1.0 if anomalies else 0.0,
        reason="embedding anomaly: " + ", ".join(anomalies) if anomalies else "",
        pillar="ai_infra",
    )
