# Compatibility and Migration Policy

## Compatibility coordinates

A deployment is compatible only when each applicable coordinate matches its
contract:

| Coordinate | Current value | Rule |
| --- | --- | --- |
| Repository source release / Rust runtime crates | `0.1.1` | All workspace runtime crates are released together with `publish = false`; this is a repository source release, not a crates.io registry publication. |
| Conformance fixture schema | `tessivum.conformance/v1` | Fixture documents must use the exact schema version. |
| WASM ABI | `cordis.plugin/v1` | The Extism host accepts this exact string only. |
| Node bridge | `cordis.node/v1` | Rust and Bun endpoints require this exact string before requests. |
| Service contract | `name@contract-version` | Chosen by the service author; independent of Core and wire versions. |

Do not infer compatibility from a shared crate version alone. For example, a
Core upgrade does not make a `service-name@2` consumer compatible with a
`service-name@1` provider, and a WASM ABI upgrade does not negotiate a Node
bridge upgrade.

## Release-version policy

`0.1.x` is the first public release line. The API is usable but still in the
pre-1.0 convergence period described by `docs/DEVELOPMENT_PLAN.md`.

- A patch release (`0.1.x` → `0.1.x+1`) preserves the documented Rust public
  APIs and the exact v1 wire semantics. It may fix a defect, tighten rejection
  of invalid input, or add an optional documented API without changing an
  existing valid wire payload.
- A breaking Rust API, Loader configuration, lifecycle semantic, or default
  behavior change advances the pre-1.0 minor version (`0.1` → `0.2`) and must
  include a migration entry and compatibility tests.
- A release that changes a protocol has its own protocol migration. Core crate
  versions do not silently bridge wire versions.
- Every release records the repository version, ABI versions, bridge version,
  and migration notes in `CHANGELOG.md`. The release evidence also records
  conformance and benchmark outputs; benchmark results are measurements for
  that release environment, not compatibility guarantees.

This policy is prospective release policy. The current implementation supports
only the exact versions in the table above; it does not provide a multi-version
runtime window.

## WASM ABI policy

`PluginManifest.abi` must equal `cordis.plugin/v1`. Manifest validation happens
before the Extism engine instantiates a guest; a v2 manifest fails
`ABI_UNSUPPORTED` rather than running with reduced functionality.

Within v1, a manifest field is not forward-compatible: `PluginManifest` uses
`deny_unknown_fields`. The current Rust request/response envelope types ignore
unknown fields, but an added envelope field is compatible only after its
semantics are documented and tested against the supported v1 host. New required
lifecycle exports, changed request/response field semantics, removed capability
names, changed capability request shapes, or changed authorization behavior
require a new ABI identifier such as `cordis.plugin/v2`.

A v2 migration must ship a v2 guest SDK and host implementation explicitly. A
host that supports both versions must expose each acceptance window explicitly;
it must not inspect an unknown ABI and guess a fallback. The v1 host in this
release supports v1 only.

### Migrating a guest to v1

1. Set `abi` to exactly `cordis.plugin/v1`.
2. Supply every manifest field: `id`, numeric three-component `version`,
   `entry`, `abi`, `inject`, `permissions`, `configSchema`, and all five
   `exports`.
3. Export `cordis_init`, `cordis_call`, `cordis_event`, `cordis_update`, and
   `cordis_stop`. Rust guests can implement `tessivum_pdk::Guest` and use
   `export_guest!`.
4. Decode `{ requestId, context, payload }` and return either
   `{ requestId, result }` or `{ requestId, error }`. The request ID must match
   exactly. The PDK emits these two forms; portable guests should not depend on
   the host's current tolerance of extra response fields.
5. Declare only needed capabilities. The host must separately grant and
   register each one; declaration alone intentionally fails with
   `PERMISSION_DENIED`.
6. Validate the deployed configuration against the declared subset schema.
   `additionalProperties: false`, `enum`, object properties, required keys, and
   array items are enforced by the host.

## Node bridge policy

The bridge version is exact, not a range and not a negotiated fallback. During
startup, the Rust client sends `hello` and accepts plugin work only after
`ready`; both frames must carry `cordis.node/v1`. The Node host records the
first connection generation and rejects every different generation. A protocol
or generation mismatch fails before plugin operations can run.

The minimum safe upgrade unit is the matched bridge crate and
`node/compat-host` source from the same repository release. Update both at the
same time, restart the `NodeSupervisor`, and discard every client and callback
handle from the old generation. Do not reconnect an old client to a restarted
host; `NodeSupervisor` generation cleanup exists specifically to prevent that.

A future bridge version (for example `cordis.node/v2`) requires a separately
implemented codec and handshake policy. It must define whether dual support is
available. V1 does not tunnel, downgrade, or reinterpret a v2 frame.

### Migrating a Node host/configuration to v1

1. Use a Bun runtime compatible with `node/compat-host` (`package.json`
   specifies Bun `>=1.3.14`) and provide vendored Cordis through
   `CORDIS_VENDOR_ROOT` or a supported checked-out vendor location.
2. Spawn the host through `HostCommand`; set a deliberate program, arguments,
   environment, working directory, and `ClientConfig` bounds/deadlines.
3. Use a four-byte big-endian length prefix and the five-field v1 JSON frame.
   Encode `connectionGeneration` and `requestId` as unsigned integer JSON
   literals, not floating-point strings or JavaScript `Number` values that have
   lost u64 precision.
4. Send `hello` first, wait for `ready`, preserve the assigned generation, and
   assign nonzero request IDs only to request/settlement kinds.
5. Send `cancel` when abandoning a request; process a cancellation as terminal
   and clean up the associated plugin, service, event, or registration state.
6. On process replacement, invoke `NodeSupervisor::shutdown()` or allow its
   disconnect cleanup to run, then create a new client and reload entries.

## Loader configuration migration

The persisted model is canonical JSON/YAML. Always write the canonical form;
input aliases are import conveniences, not an output format guarantee.

```yaml
entries:
  - package: acme/plugin
    id: acme-plugin
    runtime: legacy-node
    config: {}
    inject: []
    isolate: []
    intercept: {}
    disabled: false
```

Migration rules for v1:

- Replace `module`, `plugin`, and `source` with `package` when saving an
  existing configuration. Core accepts those aliases only while parsing.
- Replace `legacy_node` with `legacy-node` when saving. The former is a parser
  alias only.
- Give every entry one unique, nonblank identifier and every group a nonblank
  identifier. No entry ID can appear twice anywhere in the tree.
- Replace executable YAML `!!js` expressions. Core rejects them; use
  `{ "$expr": "..." }` only for the restricted `ConfigExpression` language
  evaluated against explicit `ConfigScope` inputs, or let the legacy Node
  runtime own a legacy JavaScript configuration path.
- Do not rely on entry order for service dependency activation. Use each
  runtime's declared dependencies and inspect Loader rollback errors rather
  than treating a partial candidate as committed.
- Use `EntryTree::apply`/`apply_all` for detached patch validation and
  `persist` for atomic same-directory persistence. Stop and fix a rejected
  candidate; do not edit `Loader::tree()` in place.

## Native service migration

A `ServiceKey` contains both name and contract version, and its diagnostic form
is `name@contract-version`. Changing a request/response/value contract is a
new service key version; publish a provider/consumer migration rather than
replacing a value under the old key with a different type. Native consumers
must reacquire a service after a provider replacement because old
`ServiceHandle<T>` values deliberately fail as stale.

For a staged service migration, publish the old and new keys in the provider,
update consumers to prefer the new key while still declaring an optional old
key only if that behavior is deliberate, then remove the old key in a later
release. Private and shared realms are part of service visibility; retain the
same `RealmLabel` only when consumers are intended to coactivate.

## Upgrade checklist

1. Read the release's `CHANGELOG.md`, `API.md`, `PROTOCOLS.md`, and
   `SECURITY.md`; identify all relevant compatibility coordinates.
2. Validate Loader configuration with `parse_entry_tree`/`EntryTree::validate`
   and validate WASM manifests/configuration before production activation.
3. Drain or dispose the old Loader tree and root scope. Stop WASM instances and
   shut down Node supervisors; wait for cleanup errors rather than ignoring
   them.
4. Deploy matching Core runtime crates, matching WASM guest ABI, and matching
   Node bridge/compat-host sources. Register only reviewed Native factories and
   capability handlers.
5. Start a fresh Node bridge generation, complete `hello`/`ready`, and reload
   entries. Reacquire service handles and registrations; no old-generation
   handle is valid after restart.
6. Exercise the application's actual required services, events, plugin calls,
   reloads, and shutdown path. Record the release-specific conformance and
   benchmark evidence with the deployment.
