"""Fixtures for roc system tests.

Requires PipeWire (pipewire, wireplumber, pipewire-pulse, pw-dump) and pulse
utils (pactl, paplay, parecord) on the host, plus built roc binaries in
bin/<triple>/ (override with ROC_SYSTEM_TEST_BIN). Tests are skipped when
either is missing.
"""

import os
import shutil
import subprocess

import pytest

from pwtest import PwInstance

REQUIRED_TOOLS = [
    "pipewire", "wireplumber", "pipewire-pulse",
    "pw-dump", "pactl", "paplay", "parecord",
]


def _find_bin_dir():
    if "ROC_SYSTEM_TEST_BIN" in os.environ:
        return os.environ["ROC_SYSTEM_TEST_BIN"]
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    bin_root = os.path.join(root, "bin")
    if os.path.isdir(bin_root):
        for entry in sorted(os.listdir(bin_root)):
            if os.path.exists(os.path.join(bin_root, entry, "roc-send")):
                return os.path.join(bin_root, entry)
    return None


def pytest_collection_modifyitems(config, items):
    missing = [t for t in REQUIRED_TOOLS if shutil.which(t) is None]
    reasons = []
    if missing:
        reasons.append(f"missing tools: {', '.join(missing)}")
    if _find_bin_dir() is None:
        reasons.append("roc binaries not found (build first, or set"
                       " ROC_SYSTEM_TEST_BIN)")
    if reasons:
        marker = pytest.mark.skip(reason="; ".join(reasons))
        for item in items:
            item.add_marker(marker)


@pytest.fixture
def pw(tmp_path):
    inst = PwInstance(str(tmp_path))
    inst.start()
    yield inst
    inst.stop()


@pytest.fixture(scope="session")
def roc_bin():
    return _find_bin_dir()


@pytest.fixture(scope="session")
def roc_send(roc_bin):
    return os.path.join(roc_bin, "roc-send")


@pytest.fixture(scope="session")
def roc_recv(roc_bin):
    return os.path.join(roc_bin, "roc-recv")
