"""The timing metrics at the two ends of a roc process: the hand-off with the
sound server (roc_io_*) and the departure of packets (roc_net_*). Each test
makes one process late on purpose and checks that the metric meant for that
kind of lateness moves, read from the metrics endpoint as Prometheus reads it."""

import re
import signal
import time

from pwtest import RocProc, wait_for, write_tone_raw

PKT_ENCODING = "101:pcm@s16/48000/mono"


def series(proc, name):
    """{label text: value} of every series of a metric name."""
    found = {}
    for line in proc.metrics().splitlines():
        m = re.match(rf"{re.escape(name)}(\{{[^}}]*\}})? (\S+)$", line)
        if m:
            found[m.group(1) or ""] = float(m.group(2))
    return found


def value(proc, name, labels=""):
    found = series(proc, name)
    assert labels in found, f"no series {name}{labels}; have {sorted(found)}"
    return found[labels]


def count_above(proc, name, bound, labels=""):
    """Observations of a histogram above one of its bucket bounds."""
    inner = labels.strip("{}")
    prefix = inner + "," if inner else ""
    below = value(proc, name + "_bucket", f'{{{prefix}le="{bound}"}}')
    return value(proc, name + "_count", labels) - below


def start(pw, roc_send, roc_recv, tmp_path, recv_args=()):
    pw.load_null_sink("src0", 1)
    pw.load_null_sink("dst0", 1)
    recv = RocProc(pw, roc_recv, [
        "-s", "rtp://127.0.0.1:31011",
        "--packet-encoding", PKT_ENCODING,
        "--io-encoding", "pcm@f32/48000/mono",
        "--output", "pulse://dst0",
        "--prometheus-metrics-port", "31910",
    ] + list(recv_args), "roc-recv", metrics_port=31910)
    send = RocProc(pw, roc_send, [
        "--input", "pulse://src0.monitor",
        "--io-encoding", "pcm@f32/48000/mono",
        "--packet-encoding", PKT_ENCODING,
        "-s", "rtp://127.0.0.1:31011",
        "--prometheus-metrics-port", "31911",
    ], "roc-send", metrics_port=31911)
    raw = write_tone_raw(str(tmp_path / "tone.raw"), [440.0])
    pw.play("src0", raw, channels=1)
    send.wait_metric("roc_send_packets_encoded_total")
    recv.wait_metric("roc_recv_packets_decoded_total")
    time.sleep(2.0)  # the receiver's target latency, plus settling
    return send, recv


def test_a_running_stream_fills_every_timing_metric(pw, roc_send, roc_recv, tmp_path):
    send, recv = start(pw, roc_send, roc_recv, tmp_path)
    try:
        source, sink = '{device="source"}', '{device="sink"}'
        before = {
            "interval": value(send, "roc_io_request_interval_seconds_count", source),
            "age": value(send, "roc_net_sent_packet_age_seconds_count"),
            "sink": value(recv, "roc_io_request_interval_seconds_count", sink),
        }
        time.sleep(1.0)
        assert value(send, "roc_io_request_interval_seconds_count", source) > before["interval"]
        assert value(send, "roc_io_request_size_seconds_count", source) > 0
        assert value(send, "roc_io_service_delay_seconds_count", source) > 0
        assert value(recv, "roc_io_request_interval_seconds_count", sink) > before["sink"]
        assert value(recv, "roc_io_service_delay_seconds_count", sink) > 0
        # Every audio packet is counted once when it leaves, and none of
        # them is older than the largest bound on a sender that runs freely.
        age = value(send, "roc_net_sent_packet_age_seconds_count")
        assert age > before["age"]
        assert count_above(send, "roc_net_sent_packet_age_seconds", "0.256") == 0
        mean = value(send, "roc_net_sent_packet_age_seconds_sum") / age
        assert 0 < mean < 0.256, f"mean packet age {mean}"
        # The counters exist from the start, at zero or more.
        assert value(recv, "roc_io_underflows_total", sink) >= 0
        assert value(recv, "roc_io_overflows_total", sink) >= 0
    finally:
        send.stop()
        recv.stop()


def test_a_stalled_receiver_is_counted_as_underflow(pw, roc_send, roc_recv, tmp_path):
    # With an IO latency of 200 ms the playback buffer holds 200 ms, and the
    # device gives up on its stream and opens a new one after waiting 400 ms
    # for the sound server (twice the latency). A stall of 300 ms is between
    # the two: long enough to run the buffer empty, short enough that the
    # stream survives to report it. A longer stall shows as
    # roc_io_stream_restarts_total instead.
    send, recv = start(pw, roc_send, roc_recv, tmp_path, ["--io-latency", "200ms"])
    try:
        sink = '{device="sink"}'
        before = value(recv, "roc_io_underflows_total", sink)
        restarts = value(recv, "roc_io_stream_restarts_total", sink)
        # While the receiver is stopped it writes nothing, so the sound
        # server runs its playback buffer empty.
        recv.proc.send_signal(signal.SIGSTOP)
        time.sleep(0.3)
        recv.proc.send_signal(signal.SIGCONT)
        wait_for(lambda: value(recv, "roc_io_underflows_total", sink) > before,
                 timeout=5, what="an underflow counted after the receiver was stopped")
        assert value(recv, "roc_io_stream_restarts_total", sink) == restarts
    finally:
        recv.proc.send_signal(signal.SIGCONT)
        send.stop()
        recv.stop()


def test_a_stalled_sender_sends_old_packets(pw, roc_send, roc_recv, tmp_path):
    send, recv = start(pw, roc_send, roc_recv, tmp_path)
    try:
        name = "roc_net_sent_packet_age_seconds"
        assert count_above(send, name, "0.256") == 0
        # While the sender is stopped, captured audio waits in the sound
        # server. What it sends first after resuming was captured before the
        # stop, 0.6 s earlier or more.
        send.proc.send_signal(signal.SIGSTOP)
        time.sleep(0.6)
        send.proc.send_signal(signal.SIGCONT)
        wait_for(lambda: count_above(send, name, "0.256") > 0,
                 timeout=5, what="a packet older than 256 ms after the sender was stopped")
    finally:
        send.proc.send_signal(signal.SIGCONT)
        send.stop()
        recv.stop()
