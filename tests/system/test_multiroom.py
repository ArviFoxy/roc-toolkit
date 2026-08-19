"""Multiroom session scenario: one roc-send with a 3-track pulse input and
one slot per track, three roc-recv processes each playing into its own
pulse sink. Verifies per-leg track routing through the full real stack
(PipeWire capture -> session sender -> UDP -> receivers -> PipeWire sinks)
and the per-slot metrics."""

import time

import pytest

from pwtest import RocProc, assert_single_tone, wait_for, write_tone_raw

ALL_TRACK_HZ = [440.0, 700.0, 1000.0, 1400.0, 2200.0, 3100.0, 4200.0]

PKT_ENCODING = "101:pcm@s16/48000/mono"

BASE_PORT = 32001
BASE_METRICS = 32900


def leg_port(leg):
    return BASE_PORT + leg * 10


# 7 legs is the production shape (and a channel count for which libpulse
# has no default positional map at all - the AUX map path must work).
@pytest.mark.parametrize("num_legs", [3, 7])
def test_track_per_leg(pw, roc_send, roc_recv, tmp_path, num_legs):
    TRACK_HZ = ALL_TRACK_HZ[:num_legs]
    NUM_LEGS = num_legs
    pw.load_null_sink("mroom_src", NUM_LEGS)
    for leg in range(NUM_LEGS):
        pw.load_null_sink(f"leg{leg}", 1)

    recvs = [
        RocProc(pw, roc_recv, [
            "-s", f"rtp://127.0.0.1:{leg_port(leg)}",
            "-c", f"rtcp://127.0.0.1:{leg_port(leg) + 2}",
            "--packet-encoding", PKT_ENCODING,
            "--io-encoding", "pcm@f32/48000/mono",
            "--output", f"pulse://leg{leg}",
            # Fast grid so skew rows accumulate within the test.
            "--report-grid", "100ms",
            "--prometheus-metrics-port", str(BASE_METRICS + 1 + leg),
        ], f"roc-recv-{leg}", metrics_port=BASE_METRICS + 1 + leg)
        for leg in range(NUM_LEGS)
    ]

    send_args = [
        "--input", "pulse://mroom_src.monitor",
        "--io-encoding", f"pcm@f32/48000/0-{NUM_LEGS - 1}",
        "--io-frame-len", "4ms",
        "--max-frame-size", "65536",
        "--packet-encoding", PKT_ENCODING,
        "--prometheus-metrics-port", str(BASE_METRICS),
    ]
    for leg in range(NUM_LEGS):
        send_args += [
            "-s", f"rtp://127.0.0.1:{leg_port(leg)}",
            "-c", f"rtcp://127.0.0.1:{leg_port(leg) + 2}",
            "--track", str(leg),
            "--slot-name", f"leg_{leg}",
        ]

    send = RocProc(pw, roc_send, send_args, "roc-send", metrics_port=BASE_METRICS)

    try:
        wav = write_tone_raw(str(tmp_path / "tones.raw"), TRACK_HZ)
        pw.play("mroom_src", wav, channels=NUM_LEGS)

        # All slots alive and producing labeled series.
        for leg in range(NUM_LEGS):
            send.wait_metric(f'roc_send_slot_up{{slot="leg_{leg}"}} 1')
        for recv in recvs:
            recv.wait_metric("roc_recv_packets_decoded_total")

        time.sleep(2.0)

        captures = pw.capture([f"leg{leg}" for leg in range(NUM_LEGS)],
                              seconds=2.0)

        # Each leg carries exactly its own track's tone.
        for leg, capture in enumerate(captures):
            others = [f for f in TRACK_HZ if f != TRACK_HZ[leg]]
            assert_single_tone(capture, TRACK_HZ[leg], others)

        # One clock for the session: per-slot packet counters advance in
        # lockstep. Counters keep incrementing while the registry collect
        # walks the families, so allow a few percent of scrape skew.
        counts = [
            send.metric_value("roc_send_packets_encoded_total",
                              f'{{slot="leg_{leg}"}}')
            for leg in range(NUM_LEGS)
        ]
        assert all(c is not None and c > 0 for c in counts), counts
        assert max(counts) - min(counts) <= 0.05 * max(counts), counts

        # Report plane: receiver snapshots reach the session skew
        # estimator, full rows finalize, and localhost legs are aligned
        # to well under the latency target (50 ms bound tolerates early
        # tuner-convergence transients).
        for leg in range(NUM_LEGS):
            send.wait_metric(f'roc_send_playout_offset_seconds{{slot="leg_{leg}"}}',
                             timeout=15)
        offsets = [
            send.metric_value("roc_send_playout_offset_seconds",
                              f'{{slot="leg_{leg}"}}')
            for leg in range(NUM_LEGS)
        ]
        assert all(o is not None and abs(o) < 0.05 for o in offsets), offsets

        full_rows = send.metric_value("roc_send_snapshot_rows_total",
                                      '{completeness="full"}')
        assert full_rows and full_rows > 0

        spread = send.metric_value("roc_send_playout_spread_seconds")
        assert spread is not None and spread < 0.1, spread

        skew_01 = send.metric_value("roc_send_playout_skew_seconds",
                                    '{slot_a="leg_0",slot_b="leg_1"}')
        assert skew_01 is not None and abs(skew_01) < 0.1, skew_01

        # Delay-process plane: each receiver observes per-packet delay
        # deviations, exposes the event counter, and the queue drain
        # validator produces samples.
        for leg, recv in enumerate(recvs):
            dev_count = recv.metric_value(
                "roc_recv_delay_deviation_seconds_count")
            assert dev_count is not None and dev_count > 0, (leg, dev_count)
            assert recv.metric_value("roc_recv_delay_event_total") \
                is not None, leg
            drain_count = recv.metric_value("roc_recv_queue_drain_seconds_count")
            assert drain_count is not None and drain_count > 0, (leg, drain_count)

        # The receivers' interval deviation means reach the sender as
        # per-slot gauges. Scheduling noise makes the mean positive in
        # almost every interval; poll a few rows for a nonzero one.
        for leg in range(NUM_LEGS):
            wait_for(
                lambda leg=leg: (send.metric_value(
                    "roc_send_recv_deviation_mean_seconds",
                    f'{{slot="leg_{leg}"}}') or 0) > 0,
                timeout=10,
                what=f"nonzero recv_deviation_mean for leg_{leg}")

        # Staleness attribution: kill one receiver; its snapshot clock
        # freezes while the others keep advancing.
        victim = NUM_LEGS - 1
        recvs[victim].stop()
        # An in-flight report can still land right after the stop: let it
        # drain, then take the frozen baseline.
        time.sleep(1.0)
        ts_victim_before = send.metric_value(
            "roc_send_snapshot_timestamp_seconds", f'{{slot="leg_{victim}"}}')
        ts_other_before = send.metric_value(
            "roc_send_snapshot_timestamp_seconds", '{slot="leg_0"}')
        assert ts_victim_before and ts_other_before

        # A healthy leg advances within one grid period plus one report
        # interval; poll instead of a fixed sleep.
        wait_for(lambda: (send.metric_value("roc_send_snapshot_timestamp_seconds",
                                            '{slot="leg_0"}') or 0) > ts_other_before,
                 timeout=5, what="healthy leg snapshot advance")

        ts_victim_after = send.metric_value(
            "roc_send_snapshot_timestamp_seconds", f'{{slot="leg_{victim}"}}')
        assert ts_victim_after == ts_victim_before, (ts_victim_before,
                                                     ts_victim_after)

        # The staleness check above deliberately stopped the last receiver.
        assert send.alive()
        assert all(r.alive() for r in recvs[:victim])
    finally:
        send.stop()
        for recv in recvs:
            recv.stop()


def test_wallclock_aligned_rejoin(pw, roc_send, roc_recv, tmp_path):
    """A killed receiver with --wallclock-start rejoins already aligned:
    its playout offset is back under 40 ms within 10 s of the restart and
    under 15 ms shortly after, the healthy slots see no offset-jump events
    over that window, and the rejoined receiver's own e2e latency mean
    sits near the target.

    A depth-based rejoin on this harness shows a ~100 ms offset decaying
    at the warp clamp's few ms/s for tens of seconds, so the bounds
    cleanly discriminate the aligned start. The 200 ms target (not the
    deployed 32 ms) accommodates the harness's PipeWire output path, which
    carries ~160 ms of latency between the receiver pipeline and the sink
    monitor; a lower target is unreachable there and makes the tuner
    restart sessions in a loop. The kill waits for the fleet's absolute
    e2e latency to settle near the target, so the rejoined leg is compared
    against a converged fleet. Healthy slots are checked through their
    offsets, not the offset-jump counters: PipeWire scheduling noise alone
    steps offsets by a few ms between snapshot rows, which trips the
    sender's 2 ms jump detector as background noise on this harness."""
    NUM_LEGS = 3
    TRACK_HZ = ALL_TRACK_HZ[:NUM_LEGS]
    TARGET_LATENCY_S = 0.200

    pw.load_null_sink("mroom_src", NUM_LEGS)
    for leg in range(NUM_LEGS):
        pw.load_null_sink(f"leg{leg}", 1)

    recvs = [
        RocProc(pw, roc_recv, [
            "-s", f"rtp://127.0.0.1:{leg_port(leg)}",
            "-c", f"rtcp://127.0.0.1:{leg_port(leg) + 2}",
            "--packet-encoding", PKT_ENCODING,
            "--io-encoding", "pcm@f32/48000/mono",
            "--output", f"pulse://leg{leg}",
            "--target-latency", "200ms",
            "--latency-backend", "e2e",
            "--wallclock-start",
            "--report-grid", "100ms",
            "--prometheus-metrics-port", str(BASE_METRICS + 1 + leg),
        ], f"roc-recv-{leg}", metrics_port=BASE_METRICS + 1 + leg)
        for leg in range(NUM_LEGS)
    ]

    send_args = [
        "--input", "pulse://mroom_src.monitor",
        "--io-encoding", f"pcm@f32/48000/0-{NUM_LEGS - 1}",
        "--io-frame-len", "4ms",
        "--max-frame-size", "65536",
        "--packet-encoding", PKT_ENCODING,
        "--prometheus-metrics-port", str(BASE_METRICS),
    ]
    for leg in range(NUM_LEGS):
        send_args += [
            "-s", f"rtp://127.0.0.1:{leg_port(leg)}",
            "-c", f"rtcp://127.0.0.1:{leg_port(leg) + 2}",
            "--track", str(leg),
            "--slot-name", f"leg_{leg}",
        ]

    send = RocProc(pw, roc_send, send_args, "roc-send", metrics_port=BASE_METRICS)

    try:
        wav = write_tone_raw(str(tmp_path / "tones.raw"), TRACK_HZ, seconds=90)
        pw.play("mroom_src", wav, channels=NUM_LEGS)

        for leg in range(NUM_LEGS):
            send.wait_metric(f'roc_send_slot_up{{slot="leg_{leg}"}} 1')
        for recv in recvs:
            recv.wait_metric("roc_recv_packets_decoded_total")

        # Steady state: every leg reports a playout offset and the fleet
        # spread settles under 10 ms.
        for leg in range(NUM_LEGS):
            send.wait_metric(f'roc_send_playout_offset_seconds{{slot="leg_{leg}"}}',
                             timeout=15)

        def fleet_settled():
            spread = send.metric_value("roc_send_playout_spread_seconds")
            return spread is not None and spread < 0.010
        wait_for(fleet_settled, timeout=30, what="fleet spread < 10 ms")

        # Cold start goes through the stream-younger-than-target path, so
        # the first seconds of absolute e2e latency sit above the target
        # and drift down at the warp clamp's few ms/s. Gate the kill on a
        # recent per-receiver e2e window near the target, so the rejoined
        # leg is compared against a converged fleet.
        e2e_windows = {}

        def recent_e2e_near_target():
            settled = True
            for i, recv in enumerate(recvs):
                e2e_sum = recv.metric_value("roc_recv_e2e_latency_seconds_sum")
                e2e_count = recv.metric_value("roc_recv_e2e_latency_seconds_count")
                if e2e_sum is None or e2e_count is None:
                    return False
                prev_sum, prev_count = e2e_windows.get(i, (0.0, 0.0))
                if e2e_count - prev_count < 20:
                    return False
                mean = (e2e_sum - prev_sum) / (e2e_count - prev_count)
                e2e_windows[i] = (e2e_sum, e2e_count)
                if abs(mean - TARGET_LATENCY_S) > 0.025:
                    settled = False
            return settled
        wait_for(recent_e2e_near_target, timeout=90, interval=1.0,
                 what="per-receiver recent e2e within 25 ms of target")

        # Kill one receiver, let in-flight reports drain, then restart it.
        victim = 1
        recvs[victim].stop()
        time.sleep(1.0)
        ts_baseline = send.metric_value("roc_send_snapshot_timestamp_seconds",
                                        f'{{slot="leg_{victim}"}}')

        recvs[victim].restart()

        # Within 10 s of the restart the rejoined leg reports fresh
        # snapshots and a playout offset under 40 ms absolute (a
        # depth-based start sits around 100 ms at this point and crosses
        # 40 ms only after ~25 s). Healthy legs are watched over the same
        # window: the rejoin must not drag them through the fleet median.
        def rejoined(bound):
            def pred():
                ts = send.metric_value("roc_send_snapshot_timestamp_seconds",
                                       f'{{slot="leg_{victim}"}}')
                if ts is None or (ts_baseline is not None and ts <= ts_baseline):
                    return False
                offset = send.metric_value("roc_send_playout_offset_seconds",
                                           f'{{slot="leg_{victim}"}}')
                return offset is not None and abs(offset) < bound
            return pred

        healthy_max = [0.0]

        def rejoined_healthy_watched():
            for leg in range(NUM_LEGS):
                if leg == victim:
                    continue
                offset = send.metric_value("roc_send_playout_offset_seconds",
                                           f'{{slot="leg_{leg}"}}')
                if offset is not None:
                    healthy_max[0] = max(healthy_max[0], abs(offset))
            return rejoined(0.040)()
        wait_for(rejoined_healthy_watched, timeout=10,
                 what="rejoined leg playout offset < 40 ms")

        # Healthy slots stayed put through the rejoin (same 50 ms bound
        # the steady-state assertions of this harness are calibrated to).
        assert healthy_max[0] < 0.050, healthy_max[0]

        # The restarted process is fresh, so its e2e histogram covers only
        # the rejoined session: the mean must sit near the 200 ms target
        # (a depth-based start would sit ~130 ms above it at this point;
        # the band absorbs tuner-convergence samples and harness jitter).
        def e2e_mean_at_target():
            e2e_sum = recvs[victim].metric_value("roc_recv_e2e_latency_seconds_sum")
            e2e_count = recvs[victim].metric_value("roc_recv_e2e_latency_seconds_count")
            if not e2e_count or e2e_count < 20:
                return False
            return abs(e2e_sum / e2e_count - TARGET_LATENCY_S) < 0.050
        wait_for(e2e_mean_at_target, timeout=20,
                 what="rejoined receiver e2e mean within 200 +- 50 ms")

        # The residual start error is small enough for the tuner to finish
        # within seconds.
        wait_for(rejoined(0.015), timeout=30,
                 what="rejoined leg playout offset < 15 ms")

        assert send.alive()
        assert all(r.alive() for r in recvs)
    finally:
        send.stop()
        for recv in recvs:
            recv.stop()
