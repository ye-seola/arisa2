#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

cargo ndk --version >/dev/null
cargo ndk -t arm64-v8a -t x86_64 --platform 23 build --release --locked


mkdir -p dist
cp target/aarch64-linux-android/release/arisa dist/arisa-arm64-v8a
cp target/x86_64-linux-android/release/arisa dist/arisa-x86_64
