#!/bin/sh
set -eu

cd "$(dirname "$0")"
tinygo_version=$(tinygo version)
case "$tinygo_version" in
  "tinygo version 0.43.0 "*) ;;
  *) echo "TinyGo 0.43.0 is required, got: $tinygo_version" >&2; exit 1 ;;
esac
case "$(wasm-opt --version)" in
  "wasm-opt version 133"|"wasm-opt version 133 "*) ;;
  *) echo "Binaryen 133 is required" >&2; exit 1 ;;
esac

rm -rf dist
mkdir dist

tinygo build \
  -target=wasm-unknown \
  -buildmode=c-shared \
  -scheduler=none \
  -gc=conservative \
  -panic=trap \
  -opt=2 \
  -no-debug \
  -o dist/compiler.wasm \
  .

# TinyGo's wasm-unknown runtime has no WASI imports. Resolve the deliberate
# clock probe to the trap-only module; the production Host supplies no WASI.
wasm-merge \
  dist/compiler.wasm guest \
  ../wasm-typescript/deny-wasi.wat wasi_snapshot_preview1 \
  --enable-bulk-memory \
  --enable-nontrapping-float-to-int \
  --enable-sign-ext \
  -o dist/linked.wasm

wasm-opt \
  dist/linked.wasm \
  --enable-bulk-memory \
  --strip-debug \
  --enable-nontrapping-float-to-int \
  --enable-sign-ext \
  -O2 \
  -o dist/plugin.wasm

wasm-dis dist/plugin.wasm -o dist/plugin.wat
user_imports=$(grep -c '(import "extism:host/user"' dist/plugin.wat || true)
[ "$user_imports" -eq 8 ] || {
  echo "expected exactly eight extism:host/user imports, found $user_imports" >&2
  exit 1
}
for name in \
  cordis.log cordis.config.get cordis.service.call cordis.event.emit \
  cordis.event.subscribe cordis.registration.dispose cordis.kv.get cordis.kv.set
do
  grep -Fq "(import \"extism:host/user\" \"$name\"" dist/plugin.wat || {
    echo "missing Host Function import: $name" >&2
    exit 1
  }
done
if grep -q '(import "wasi_' dist/plugin.wat; then
  echo "WASI imports remain after deny-module linkage" >&2
  exit 1
fi
for name in cordis_init cordis_call cordis_event cordis_update cordis_stop
do
  grep -Fq "(export \"$name\"" dist/plugin.wat || {
    echo "missing guest export: $name" >&2
    exit 1
  }
done
rm dist/plugin.wat

artifact_bytes=$(wc -c < dist/plugin.wasm)
[ "$artifact_bytes" -le 8388608 ] || {
  echo "plugin.wasm exceeds the 8 MiB package limit" >&2
  exit 1
}
