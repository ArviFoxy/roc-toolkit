# System tests

End-to-end tests that run real `roc-send`/`roc-recv` processes against a
private PipeWire instance: pulse capture into the sender, localhost UDP,
pulse playback out of the receivers, with per-track tone verification and
Prometheus metric assertions.

## Running

```
scons -Q --enable-prometheus --build-3rdparty=openfec   # metrics required
python3 -m pytest tests/system -v
```

Requirements: `pipewire`, `wireplumber`, `pipewire-pulse`, pw-utils
(`pw-dump`, `pw-play`, `pw-record`), `pactl`, `python-pytest`,
`python-numpy`. Tests skip themselves when tools or binaries are missing.
`ROC_SYSTEM_TEST_BIN` overrides the roc binary directory.

## Isolation

The harness never touches a session PipeWire:

- Each test gets a fresh instance (`pipewire`, `wireplumber`,
  `pipewire-pulse`) in a private `XDG_RUNTIME_DIR` under the pytest temp
  dir; sockets are only discoverable through that environment.
- Subprocess environments are built from an allowlist, never copied from
  the caller: a process that escapes the harness env finds *no* daemon
  instead of silently falling back to the session instance
  (`test_isolation.py` keeps this contract executable).
- The daemon configs are hardware-blind: all device monitors are disabled,
  so the instance cannot see or claim sound cards; D-Bus features are off.
- The instance carries a `test.marker` property that the harness asserts
  after startup.

On failure, per-process logs and captured wavs remain in the pytest temp
dir (`/tmp/pytest-of-<user>/...`).

## Sharp edges encoded in the harness

- Streams are routed with `target.object` (object serial): WirePlumber
  ignores pw-cat's legacy `--target` node-id property and would silently
  route to the default sink.
- Multitrack roc streams use AUX channel maps (libpulse has no default
  positional map for every channel count - seven has none at all - and
  positional maps invite remixing); sinks feeding a multitrack capture
  must be aux-mapped too, or pipewire's channelmix positionally remixes,
  smearing tracks together. pactl spells the positions lowercase
  (`aux0`), pw-cat uppercase (`AUX0`).
- Harness signal I/O uses native `pw-play`/`pw-record` with RAW samples;
  the pulse layer is exercised by roc itself, which is the code under
  test, and wav files carry positional channel meanings that pw-play
  would remap onto the stream positions.
