import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

process.chdir(dirname(fileURLToPath(import.meta.url)));

const sdk = resolve(process.env.WASI_SDK_PATH || "../../target/w3-tools/wasi-sdk-34.0-arm64-macos");
const micropython = resolve(process.env.MICROPYTHON_DIR || "../../target/w3-tools/micropython-1.26.1");
const clang = join(sdk, "bin", "clang");
const wasmOpt = process.env.WASM_OPT || "wasm-opt";
const mpyCross = process.env.MPY_CROSS || "mpy-cross";
const run = (command, args, options = {}) => execFileSync(command, args, { stdio: "inherit", ...options });
const version = (command, args = ["--version"]) => execFileSync(command, args, { encoding: "utf8" }).trim();

assert.match(version(clang), /clang version/, "WASI_SDK_PATH must name WASI SDK 34");
assert.equal((await readFile(join(sdk, "VERSION"), "utf8")).split("\n", 1)[0], "34.0", "WASI SDK 34.0 is required");
assert.match(version(wasmOpt), /version 133\b/, "Binaryen 133 is required");
assert.match(version(mpyCross), /MicroPython v1\.26\.1\b/, "mpy-cross 1.26.1 is required");
const config = await readFile(join(micropython, "py", "mpconfig.h"), "utf8");
assert.match(config, /MICROPY_VERSION_MAJOR 1\b/);
assert.match(config, /MICROPY_VERSION_MINOR 26\b/);
assert.match(config, /MICROPY_VERSION_MICRO 1\b/);

await rm("dist", { recursive: true, force: true });
await mkdir("dist", { recursive: true });
run(mpyCross, ["-msmall-int-bits=31", "-o", "dist/plugin.mpy", "plugin.py"]);
const source = await readFile("dist/plugin.mpy");
const sourceBytes = Array.from(source, byte => String(byte)).join(",");
await writeFile(
  "dist/plugin_source.h",
  `static const unsigned char plugin_source[] = {${sourceBytes}};\nstatic const size_t plugin_source_len = sizeof(plugin_source);\n`,
);

const target = "--target=wasm32-wasip1";
const sysroot = `--sysroot=${join(sdk, "share", "wasi-sysroot")}`;
run("make", [
  "-f", "micropython_embed.mk",
  `MICROPYTHON_TOP=${micropython}`,
  "BUILD=dist/build-embed",
  "PACKAGE_DIR=dist/micropython_embed",
  `QSTR_DEFS=${resolve("qstrdefs.h")}`,
  `CC=${clang} ${target} ${sysroot} -mllvm -wasm-enable-sjlj`,
]);

const generated = resolve("dist/micropython_embed");
const pySources = (await readdir(join(generated, "py")))
  .filter(name => name.endsWith(".c"))
  .map(name => join(generated, "py", name));
const raw = resolve("dist/plugin.raw.wasm");
run(clang, [
  target,
  sysroot,
  "-mllvm", "-wasm-enable-sjlj",
  "-std=c99",
  "--no-wasm-opt",
  "-Oz",
  "-DNDEBUG",
  "-ffunction-sections",
  "-fdata-sections",
  "-fvisibility=hidden",
  "-nostartfiles",
  `-I${resolve(".")}`,
  `-I${resolve("dist")}`,
  `-I${generated}`,
  `-I${join(generated, "port")}`,
  ...pySources,
  join(generated, "shared", "runtime", "gchelper_generic.c"),
  join(micropython, "extmod", "modjson.c"),
  resolve("guest.c"),
  "-lsetjmp",
  "-Wl,--no-entry",
  "-Wl,--export-memory",
  "-Wl,--gc-sections",
  "-Wl,--initial-memory=2097152",
  "-Wl,--max-memory=67108864",
  "-Wl,-z,stack-size=262144",
  "-o", raw,
]);
run(wasmOpt, [raw, "--enable-exception-handling", "-Oz", "--spill-pointers", "--strip", "--emit-exnref", "-o", "dist/plugin.wasm"]);

const bytes = await readFile("dist/plugin.wasm");
assert.ok(bytes.length <= 8 * 1024 * 1024, "Python artifact exceeds the unchanged 8 MiB admission limit");
const module = new WebAssembly.Module(bytes);
const imports = WebAssembly.Module.imports(module);
const hostNames = [
  "cordis.log",
  "cordis.config.get",
  "cordis.service.call",
  "cordis.event.emit",
  "cordis.event.subscribe",
  "cordis.registration.dispose",
  "cordis.kv.get",
  "cordis.kv.set",
];
assert.ok(imports.every(item => item.kind === "function" && (
  item.module === "extism:host/env" || (item.module === "extism:host/user" && hostNames.includes(item.name))
)), "unexpected imports (including WASI) are forbidden");
assert.deepEqual(
  imports.filter(item => item.module === "extism:host/user").map(item => item.name).sort(),
  hostNames.slice().sort(),
);
const exports = new Set(WebAssembly.Module.exports(module).filter(item => item.kind === "function").map(item => item.name));
for (const name of ["init", "call", "event", "update", "stop"]) {
  assert.ok(exports.has(`cordis_${name}`));
}
await writeFile(
  "dist/plugin.wasm.sha256",
  `${createHash("sha256").update(bytes).digest("hex")}  plugin.wasm\n`,
);
console.log(`Built ${bytes.length} bytes with MicroPython 1.26.1; five ABI exports, eight exact Host imports, no WASI imports.`);
