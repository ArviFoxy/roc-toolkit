#!/usr/bin/env bash

set -euxo pipefail

# System tests: real roc-send/roc-recv processes against an isolated
# PipeWire instance (see tests/system/README.md). Requires pipewire,
# wireplumber, pipewire-pulse, pipewire-utils, pulseaudio-utils,
# python-pytest and python-numpy; tests skip themselves when missing.

scons -Q \
      --enable-prometheus \
      --build-3rdparty=openfec

python3 -m pytest tests/system -v
