"""Multiroom session scenario: one roc-send with a 3-track pulse input and
one slot per track, three roc-recv processes each playing into its own
pulse sink. Verifies per-leg track routing through the full real stack
(PipeWire capture -> session sender -> UDP -> receivers -> PipeWire sinks)
and the per-slot metrics."""

import time

from pwtest import RocProc, assert_single_tone, write_tone_wav

TRACK_HZ = [440.0, 1000.0, 2200.0]
NUM_LEGS = len(TRACK_HZ)

PKT_ENCODING = "101:pcm@s16/48000/mono"

BASE_PORT = 32001
BASE_METRICS = 32900


def leg_port(leg):
    return BASE_PORT + leg * 10


def test_track_per_leg(pw, roc_send, roc_recv, tmp_path):
    pw.load_null_sink("mroom_src", NUM_LEGS)
    for leg in range(NUM_LEGS):
        pw.load_null_sink(f"leg{leg}", 1)

    recvs = [
        RocProc(pw, roc_recv, [
            "-s", f"rtp://127.0.0.1:{leg_port(leg)}",
            "--packet-encoding", PKT_ENCODING,
            "--io-encoding", "pcm@f32/48000/mono",
            "--output", f"pulse://leg{leg}",
            "--prometheus-metrics-port", str(BASE_METRICS + 1 + leg),
        ], f"roc-recv-{leg}", metrics_port=BASE_METRICS + 1 + leg)
        for leg in range(NUM_LEGS)
    ]

    send_args = [
        "--input", "pulse://mroom_src.monitor",
        "--io-encoding", f"pcm@f32/48000/0-{NUM_LEGS - 1}",
        "--io-frame-len", "4ms",
        "--packet-encoding", PKT_ENCODING,
        "--prometheus-metrics-port", str(BASE_METRICS),
    ]
    for leg in range(NUM_LEGS):
        send_args += [
            "-s", f"rtp://127.0.0.1:{leg_port(leg)}",
            "--track", str(leg),
            "--slot-name", f"leg_{leg}",
        ]

    send = RocProc(pw, roc_send, send_args, "roc-send", metrics_port=BASE_METRICS)

    try:
        wav = write_tone_wav(str(tmp_path / "tones.wav"), TRACK_HZ)
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

        # One clock for the session: per-slot packet counters stay equal.
        counts = [
            send.metric_value("roc_send_packets_encoded_total",
                              f'{{slot="leg_{leg}"}}')
            for leg in range(NUM_LEGS)
        ]
        assert all(c is not None and c > 0 for c in counts), counts
        assert max(counts) - min(counts) <= 2, counts

        assert send.alive() and all(r.alive() for r in recvs)
    finally:
        send.stop()
        for recv in recvs:
            recv.stop()
