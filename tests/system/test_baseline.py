"""Baseline end-to-end scenario: one mono stream through real PipeWire on
both ends (pulse source into roc-send, roc-recv into a pulse sink), over
localhost UDP, with real processes and real clocks."""

import os

from pwtest import RocProc, assert_single_tone, write_tone_wav

TONE_HZ = 440.0
OTHER_HZ = [1000.0, 2200.0]

PKT_ENCODING = "101:pcm@s16/48000/mono"


def test_mono_loopback(pw, roc_send, roc_recv, tmp_path):
    pw.load_null_sink("src0", 1)
    pw.load_null_sink("dst0", 1)

    recv = RocProc(pw, roc_recv, [
        "-s", "rtp://127.0.0.1:31001",
        "--packet-encoding", PKT_ENCODING,
        "--io-encoding", "pcm@f32/48000/mono",
        "--output", "pulse://dst0",
        "--prometheus-metrics-port", "31900",
    ], "roc-recv", metrics_port=31900)

    send = RocProc(pw, roc_send, [
        "--input", "pulse://src0.monitor",
        "--io-encoding", "pcm@f32/48000/mono",
        "--packet-encoding", PKT_ENCODING,
        "-s", "rtp://127.0.0.1:31001",
        "--prometheus-metrics-port", "31901",
    ], "roc-send", metrics_port=31901)

    try:
        wav = write_tone_wav(str(tmp_path / "tone.wav"), [TONE_HZ])
        pw.play("src0", wav)

        send.wait_metric("roc_send_packets_encoded_total")
        recv.wait_metric("roc_recv_packets_decoded_total")

        # Warmup: default target latency plus stream settle.
        import time
        time.sleep(2.0)

        (capture,) = pw.capture(["dst0"], seconds=2.0)
        assert_single_tone(capture, TONE_HZ, OTHER_HZ)

        assert send.alive() and recv.alive()
        assert recv.metric_value("roc_recv_packets_decoded_total") > 0
    finally:
        send.stop()
        recv.stop()
