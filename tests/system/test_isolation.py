"""The isolation contract itself, kept as executable documentation."""

import os
import subprocess

from pwtest import TEST_MARKER


def test_instance_is_marked(pw):
    # We are talking to the test instance, not to a session daemon: the
    # marker property only exists in the test config.
    assert pw.core_has_marker()


def test_no_fallback_without_env(pw, tmp_path):
    # A client that loses the isolated environment must find NO daemon at
    # all (the allowlist never includes the caller's XDG_RUNTIME_DIR or any
    # PULSE_*/PIPEWIRE_* variables), instead of silently reaching a session
    # instance.
    empty_rt = tmp_path / "empty-rt"
    empty_rt.mkdir()
    r = subprocess.run(
        ["pactl", "info"],
        env={"PATH": os.environ["PATH"], "XDG_RUNTIME_DIR": str(empty_rt)},
        capture_output=True,
    )
    assert r.returncode != 0


def test_instance_sees_no_hardware(pw):
    # The device monitors are disabled in config: no ALSA/bluez node can
    # ever appear, so the instance cannot compete with a session daemon
    # for sound cards.
    for name in pw.node_names():
        assert not name.startswith("alsa_"), f"hardware node leaked: {name}"
        assert not name.startswith("bluez_"), f"hardware node leaked: {name}"
