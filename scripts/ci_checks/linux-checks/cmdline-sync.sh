#!/usr/bin/env bash

# Verifies that the tracked gengetopt output (cmdline.c/cmdline.h)
# matches the .ggo sources. The build compiles the tracked files, so a
# .ggo edit without a matching regeneration gives the option parser one
# struct layout and main.cpp another - a silent memory corruption. Run
# this after any .ggo change; regenerate with the build system (scons
# regenerates into the build dir; copy the pair back into the tree).

set -euo pipefail
cd "$(dirname "$0")/../../.."

fail=0
for tool in roc_recv roc_send; do
    dir="src/tools/$tool"
    tmp="$(mktemp -d)"
    gengetopt -i "$dir/cmdline.ggo" -F cmdline --output-dir "$tmp" \
        --set-version check

    # The version string and the generation-command comment differ per
    # invocation; everything else must match exactly.
    filter='CMDLINE_PARSER_VERSION|gengetopt |output-dir'
    for f in cmdline.c cmdline.h; do
        if ! diff -u \
            <(grep -Ev "$filter" "$tmp/$f") \
            <(grep -Ev "$filter" "$dir/$f") > /dev/null; then
            echo "MISMATCH: $dir/$f is out of sync with $dir/cmdline.ggo" >&2
            fail=1
        fi
    done
    rm -rf "$tmp"
done

if [ "$fail" -ne 0 ]; then
    echo "Regenerate the cmdline pair from the .ggo sources." >&2
fi
exit "$fail"
