import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

process.chdir(dirname(fileURLToPath(import.meta.url)));
const run = (command, args) => execFileSync(command, args, { stdio: "inherit" });
await mkdir("dist", { recursive: true });
await rm("dist/plugin.wasm", { force: true });
run("node", ["node_modules/typescript/bin/tsc", "--noEmit", "--strict", "--lib", "es2020", "--types", "@extism/js-pdk", "src/index.ts", "plugin.d.ts"]);
await build({ entryPoints: ["src/index.ts"], bundle: true, format: "cjs", platform: "neutral", target: "es2020", outfile: "dist/plugin.js" });

// extism-js accepts identifier method names only. Link those private bindings to
// the exact ABI names; the Host never exposes the compiler aliases.
const declarations = await readFile("plugin.d.ts", "utf8");
const names = [...declarations.matchAll(/"(cordis\.[a-z.]+)"/g)].map(match => match[1]);
assert.equal(new Set(names).size, 8, "all eight Host Functions must be declared");
const alias = name => name.replaceAll(".", "_");
await writeFile("dist/compiler.d.ts", declarations.replace(/"(cordis\.[a-z.]+)"/g, (_, name) => alias(name)));
await writeFile("dist/host-bindings.wat", `(module\n${names.map((name, i) =>
  `  (import "extism:host/user" "${name}" (func $host${i} (param i64) (result i64)))`
).join("\n")}\n${names.map((name, i) =>
  `  (func (export "${alias(name)}") (param i64) (result i64) (call $host${i} (local.get 0)))`
).join("\n")}\n)\n`);
run("extism-js", ["dist/plugin.js", "-i", "dist/compiler.d.ts", "-o", "dist/compiler.wasm"]);
run("wasm-merge", ["dist/compiler.wasm", "guest", "dist/host-bindings.wat", "extism:host/user", "deny-wasi.wat", "wasi_snapshot_preview1", "--enable-reference-types", "--enable-bulk-memory", "-o", "dist/linked.wasm"]);
run("wasm-opt", ["dist/linked.wasm", "--enable-reference-types", "--enable-bulk-memory", "--strip-debug", "-O2", "-o", "dist/validated.wasm"]);
const bytes = await readFile("dist/validated.wasm");
const module = new WebAssembly.Module(bytes);
const imports = WebAssembly.Module.imports(module);
assert.ok(imports.every(item => item.kind === "function" && (
  item.module === "extism:host/env" || (item.module === "extism:host/user" && names.includes(item.name))
)), "unexpected imports (including WASI) are forbidden");
assert.deepEqual(imports.filter(item => item.module === "extism:host/user").map(item => item.name).sort(), names.sort());
const exports = new Set(WebAssembly.Module.exports(module).filter(item => item.kind === "function").map(item => item.name));
for (const name of ["init", "call", "event", "update", "stop"]) assert.ok(exports.has(`cordis_${name}`));
await writeFile("dist/plugin.wasm", bytes);
await writeFile("dist/plugin.wasm.sha256", `${createHash("sha256").update(bytes).digest("hex")}  plugin.wasm\n`);
console.log(`Built ${bytes.length} bytes; five ABI exports, eight exact Host imports, no WASI imports.`);
