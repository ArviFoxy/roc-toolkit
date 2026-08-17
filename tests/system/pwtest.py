"""Helpers for roc system tests: an isolated PipeWire instance, roc process
management, and signal generation/analysis.

Isolation model ("level 1.5"): every subprocess environment is BUILT FROM AN
ALLOWLIST, never copied from os.environ, so a process that somehow escapes the
harness finds no daemon at all instead of silently falling back to the user's
session PipeWire. The daemon configs are hardware-blind (no device monitors),
so the test instance cannot touch sound cards either way.
"""

import json
import os
import shutil
import subprocess
import time
import urllib.request
import wave

import numpy as np

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")

TEST_MARKER = "roc-system-tests"


class TimeoutError_(Exception):
    pass


def wait_for(pred, timeout, interval=0.1, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError_(f"timed out after {timeout}s waiting for {what}")


class PwInstance:
    """A private PipeWire + WirePlumber + pipewire-pulse tree in a temp dir."""

    def __init__(self, tmpdir):
        self.dir = tmpdir
        self.procs = []
        self.logs = {}

        rt = os.path.join(tmpdir, "rt")
        os.makedirs(rt, mode=0o700)
        for sub in ("home", "config", "state", "cache", "logs"):
            os.makedirs(os.path.join(tmpdir, sub), exist_ok=True)

        # WirePlumber: WIREPLUMBER_CONFIG_DIR replaces the whole config
        # search path, and the stock wireplumber.conf carries required
        # machinery (client modules, components). Use the host's stock file
        # and merge our hardware-blind toggles in as a .conf.d fragment.
        wp_dir = os.path.join(tmpdir, "wp-config")
        wp_frag_dir = os.path.join(wp_dir, "wireplumber.conf.d")
        os.makedirs(wp_frag_dir)
        shutil.copy("/usr/share/wireplumber/wireplumber.conf", wp_dir)
        shutil.copy(os.path.join(CONF_DIR, "wireplumber", "99-roc-tests.conf"),
                    wp_frag_dir)

        # The allowlist. Deliberately absent: DBUS_SESSION_BUS_ADDRESS,
        # PULSE_*, PIPEWIRE_* and everything else from the caller.
        self.env = {
            "PATH": os.environ["PATH"],
            "HOME": os.path.join(tmpdir, "home"),
            "XDG_RUNTIME_DIR": rt,
            "XDG_CONFIG_HOME": os.path.join(tmpdir, "config"),
            "XDG_STATE_HOME": os.path.join(tmpdir, "state"),
            "XDG_CACHE_HOME": os.path.join(tmpdir, "cache"),
            "PIPEWIRE_CONFIG_DIR": os.path.join(CONF_DIR, "pipewire"),
            "WIREPLUMBER_CONFIG_DIR": wp_dir,
        }

    # -- process management --

    def spawn(self, cmd, name):
        log = open(os.path.join(self.dir, "logs", name + ".log"), "wb")
        proc = subprocess.Popen(cmd, env=self.env, stdout=log, stderr=log)
        self.procs.append((name, proc, log))
        self.logs[name] = log.name
        return proc

    def run(self, cmd, check=True, timeout=10):
        return subprocess.run(cmd, env=self.env, capture_output=True, text=True,
                              check=check, timeout=timeout)

    def start(self):
        self.spawn(["pipewire"], "pipewire")
        self.spawn(["wireplumber"], "wireplumber")
        self.spawn(["pipewire-pulse"], "pipewire-pulse")

        wait_for(self.core_has_marker, timeout=10, what="pipewire core with marker")
        wait_for(self.pulse_is_up, timeout=10, what="pulse socket")

    def stop(self):
        for name, proc, log in reversed(self.procs):
            if proc.poll() is None:
                proc.terminate()
        for name, proc, log in reversed(self.procs):
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            log.close()
        self.procs = []

    # -- state inspection --

    def dump(self):
        r = self.run(["pw-dump"], check=False)
        if r.returncode != 0 or not r.stdout.strip():
            return []
        return json.loads(r.stdout)

    def core_has_marker(self):
        for obj in self.dump():
            props = obj.get("info", {}).get("props", {}) or {}
            if props.get("test.marker") == TEST_MARKER:
                return True
        return False

    def pulse_is_up(self):
        return self.run(["pactl", "info"], check=False).returncode == 0

    def node_names(self):
        names = []
        for obj in self.dump():
            props = obj.get("info", {}).get("props", {}) or {}
            if "node.name" in props:
                names.append(props["node.name"])
        return names

    # -- graph objects --

    # Channel maps agreeing with what roc's pulse:// endpoints negotiate:
    # AUX for multitrack streams (positionless tracks; exists for any
    # count), MONO for mono. Sink, player and roc must agree on positions,
    # or pipewire's channelmix remixes on each hop: tracks smear together
    # and LFE-mapped channels vanish entirely.
    # The two tools spell channel names differently: pactl wants pulse
    # names (lowercase), pw-cat wants pipewire names (uppercase).
    @staticmethod
    def chmap_pulse(channels):
        if channels == 1:
            return "mono"
        return ",".join(f"aux{i}" for i in range(channels))

    @staticmethod
    def chmap_pw(channels):
        if channels == 1:
            return "MONO"
        return ",".join(f"AUX{i}" for i in range(channels))

    def load_null_sink(self, name, channels):
        self.run([
            "pactl", "load-module", "module-null-sink",
            f"sink_name={name}", f"channels={channels}",
            f"channel_map={self.chmap_pulse(channels)}",
        ])
        wait_for(lambda: name in self.node_names(), timeout=5,
                 what=f"null sink {name}")

    # -- signal I/O --
    #
    # Test signal transport uses the native pw-play/pw-record tools: the
    # pulse layer is exercised by roc itself (pulse:// endpoints), which is
    # the code under test, while the harness I/O should be maximally boring.

    def node_serial(self, name):
        # WirePlumber routes streams via the target.object property, which
        # takes an object.serial (pw-cat's legacy --target node-id prop is
        # ignored and streams silently land on the default sink).
        for obj in self.dump():
            props = obj.get("info", {}).get("props", {}) or {}
            if props.get("node.name") == name:
                return props["object.serial"]
        raise AssertionError(f"node {name!r} not found")

    def play(self, sink, raw_path, channels=1, rate=48000):
        """Starts playback of raw f32 samples into a sink.

        Raw on purpose: a wav file carries positional channel meanings that
        pw-play maps onto the stream positions, remixing multichannel
        signals; raw samples are copied 1:1 into the declared channels.
        """
        target = str(self.node_serial(sink))
        return self.spawn(["pw-play", "--raw", "--format=f32",
                           "--rate", str(rate), "--channels", str(channels),
                           "--channel-map", self.chmap_pw(channels),
                           "-P", f"target.object={target}", raw_path],
                          f"pw-play-{sink}")

    def capture(self, sinks, seconds):
        """Records the monitors of the given sinks concurrently."""
        procs = []
        paths = []
        for sink in sinks:
            path = os.path.join(self.dir, f"capture-{sink}.wav")
            log = open(os.path.join(self.dir, "logs", f"pw-record-{sink}.log"), "wb")
            target = str(self.node_serial(sink))
            procs.append((subprocess.Popen(
                ["pw-record", "-P", f"target.object={target}",
                 "-P", "stream.capture.sink=true", path],
                env=self.env, stdout=log, stderr=log), log))
            paths.append(path)
        time.sleep(seconds)
        for proc, log in procs:
            proc.terminate()
        for proc, log in procs:
            proc.wait(timeout=5)
            log.close()
        return paths


class RocProc:
    """A roc-send/roc-recv process inside the isolated environment."""

    def __init__(self, pw, binary, args, name, metrics_port=None):
        self.pw = pw
        self.name = name
        self.metrics_port = metrics_port
        self.proc = pw.spawn([binary] + args, name)

    def alive(self):
        return self.proc.poll() is None

    def metrics(self):
        url = f"http://127.0.0.1:{self.metrics_port}/metrics"
        with urllib.request.urlopen(url, timeout=2) as resp:
            return resp.read().decode()

    def wait_metric(self, needle, timeout=10):
        def pred():
            try:
                return needle in self.metrics()
            except OSError:
                return False
        wait_for(pred, timeout, what=f"metric {needle!r} from {self.name}")

    def metric_value(self, name, labels=""):
        for line in self.metrics().splitlines():
            if line.startswith(name + labels + " "):
                return float(line.split()[-1])
        return None

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)


# -- signal generation and analysis --

def write_tone_raw(path, freqs, rate=48000, seconds=30, amplitude=0.5):
    """Raw interleaved f32 where channel i carries a pure tone at freqs[i]."""
    t = np.arange(int(rate * seconds)) / rate
    chans = [amplitude * np.sin(2 * np.pi * f * t) for f in freqs]
    data = np.stack(chans, axis=1).astype("<f4")
    with open(path, "wb") as f:
        f.write(data.tobytes())
    return path


def write_tone_wav(path, freqs, rate=48000, seconds=30, amplitude=0.5):
    """Multichannel wav where channel i carries a pure tone at freqs[i]."""
    t = np.arange(int(rate * seconds)) / rate
    chans = [amplitude * np.sin(2 * np.pi * f * t) for f in freqs]
    data = np.stack(chans, axis=1)
    pcm = (data * 32767).astype("<i2")
    with wave.open(path, "wb") as w:
        w.setnchannels(len(freqs))
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm.tobytes())
    return path


def read_wav_mono(path):
    with wave.open(path, "rb") as w:
        rate = w.getframerate()
        n = w.getnframes()
        raw = w.readframes(n)
        data = np.frombuffer(raw, dtype="<i2").astype(np.float64)
        if w.getnchannels() > 1:
            data = data.reshape(-1, w.getnchannels()).mean(axis=1)
    return rate, data / 32768.0


def tone_powers(path, candidate_freqs, skip_seconds=0.25, band_hz=20.0):
    """Power near each candidate frequency in a captured mono wav."""
    rate, data = read_wav_mono(path)
    data = data[int(rate * skip_seconds):]
    if len(data) < rate // 4:
        raise AssertionError(f"capture too short: {path}")
    spectrum = np.abs(np.fft.rfft(data * np.hanning(len(data)))) ** 2
    freq_axis = np.fft.rfftfreq(len(data), d=1.0 / rate)
    powers = {}
    for f in candidate_freqs:
        band = (freq_axis > f - band_hz) & (freq_axis < f + band_hz)
        powers[f] = float(spectrum[band].sum())
    return powers


def assert_single_tone(path, expected, others, min_ratio_db=20.0):
    """Asserts the capture contains `expected` and not the `others`."""
    powers = tone_powers(path, [expected] + list(others))
    exp_power = powers[expected]
    assert exp_power > 0, f"{path}: no signal at {expected} Hz"
    for f in others:
        ratio_db = 10 * np.log10(exp_power / max(powers[f], 1e-30))
        assert ratio_db >= min_ratio_db, (
            f"{path}: tone {f} Hz too strong vs expected {expected} Hz:"
            f" {ratio_db:.1f} dB < {min_ratio_db} dB (powers={powers})")
