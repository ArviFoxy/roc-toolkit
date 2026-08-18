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
