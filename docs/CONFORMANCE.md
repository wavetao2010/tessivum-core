# M0 Conformance Contract

## Scope and authority

M0 defines a language-neutral behavioral baseline before the Rust framework exists. For a supported fixture, the TypeScript oracle runs the Harness-vendored Cordis behavior and emits its normalized trace. The Rust runner validates the fixture and returns `NOT_IMPLEMENTED`; it does not claim equivalence yet. Loader scenarios are deliberately unsupported in M0 and the TypeScript oracle returns `UNSUPPORTED_SCENARIO` for them.

When sources disagree, resolve behavior in this order:

1. DeepSeek Harness observable behavior;
2. Harness vendored-patch contract;
3. Cordis upstream behavior;
4. private TypeScript implementation detail.

## Source anchors

| Anchor | Pinned revision | Use |
|---|---|---|
| [Cordis](https://github.com/cordiverse/cordis/tree/47f943859bef60e4160492346772ded9b24f765a) | `47f943859bef60e4160492346772ded9b24f765a` | Upstream event, context, registry, fiber, reflect, and service behavior. |
| [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness/tree/8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4) | `8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4` | Observable Harness behavior and `vendor/README.md` patch record. |
| Harness vendored Cordis | `56b3d4f725681cf4556c1a8695a709cc3b6eed74` | Exact oracle baseline. |
| `docs/DEVELOPMENT_PLAN.md` §§3, 7, 8/M0, 10 | repository-local | Contract priority, invariants, M0 boundary, and test matrix. |

The anchors are intentionally revision-pinned: updating one requires a new oracle baseline and an explicit ledger review.

## Fixture schema

Every fixture is JSON with this shape:

```json
{
  "schemaVersion": "tessivum.conformance/v1",
  "name": "unique-fixture-name",
  "domain": "lifecycle",
  "scenario": "scenario-name",
  "input": {},
  "expectedTrace": []
}
```

Rules:

- `schemaVersion` is exactly `tessivum.conformance/v1`.
- `name` is unique across `fixtures/conformance/`.
- `domain` is one of `lifecycle`, `service`, `event`, or `loader`.
- `scenario` selects the domain behavior; `input` is optional and scenario-owned.
- `expectedTrace` is required, ordered, and exact after normalization. The runner must reject malformed entries rather than silently ignoring fields.
- Each record identifies only fixture-local subjects. Runtime IDs, addresses, and random fiber IDs are never fixture data.

A trace record has an event name and only applicable optional fields:

```text
{ event, subject?, from?, to?, label?, phase?, value?, error? }
```

`subject`, `from`, `to`, `label`, and `phase` are stable fixture-local strings. `value` is a JSON value after canonical normalization. `error` is a stable error classification/message selected by the scenario, never a stack trace.

## Trace events

| Event | Meaning and stable fields |
|---|---|
| `fiber-created` | A fixture-local fiber begins. `subject` identifies it; `label` may identify its owner or plugin. |
| `fiber-state-changed` | One committed lifecycle transition. `subject`, `from`, and `to` identify the transition; `phase` identifies the operation when needed. |
| `service-provided` | A service becomes visible in its realm. `subject` identifies the service key; `label` may identify the realm/provider generation. |
| `service-removed` | A previously visible service stops being visible. `subject` identifies the service key; `label` may identify the realm/provider generation. |
| `listener-added` | A listener is owned by a scope. `subject` identifies the event; `label` identifies the local listener. |
| `listener-removed` | That owned listener is no longer dispatchable. `subject` and `label` match its normalized registration. |
| `event-dispatched` | An event mode dispatches. `subject` identifies the event, `label` the mode, and `value` the normalized arguments/result when scenario-relevant. |
| `effect-created` | A cleanup effect is registered. `subject` identifies the local effect; `label` may identify its owner. |
| `effect-disposed` | A registered cleanup effect settles. `subject` identifies the same local effect. |
| `plugin-error` | A plugin/fiber operation fails. `subject` identifies it; `phase` and `error` are stable diagnostic values. |
| `config-committed` | A configuration candidate becomes current. `subject` identifies the config root; `value` may carry normalized config identity. |
| `config-rolled-back` | A configuration candidate fails and the prior good state remains. `subject`, `phase`, and `error` identify the failure. |

No other event names are valid in M0 fixtures.

## Normalization and comparison

The oracle normalizes before comparison so independent implementations compare behavior rather than implementation artifacts:

1. Keep the observed event order.
2. Replace runtime identities with fixture-local subjects and labels.
3. Keep only the fields defined above; omit absent fields rather than encoding host-language `undefined`/`null` distinctions that the scenario does not observe.
4. Canonicalize JSON values structurally (object-key order is not significant); preserve array order.
5. Reduce errors to scenario-stable `phase` and `error`; remove stacks, paths, addresses, timestamps, and engine-specific wording.
6. Compare every normalized record in order. Extra, missing, reordered, or changed records are failures.

A fixture may observe a deterministic ordering guarantee, but not use a trace to assert scheduler accidents. Nested cleanup is asserted in reverse registration order. Independent top-level cleanup may be concurrent and must not be ordered by this contract.

## Behavioral rules covered by M0

### Lifecycle

- A resource has one owner scope.
- Entering unloading rejects new effects, listeners, services, and child fibers.
- Disposal is idempotent; concurrent/reentrant callers join one cleanup operation.
- Child scopes quiesce before their parent completes; nested cleanup is reverse-order.
- Failed startup rolls back every resource created during startup and reaches `Failed` rather than leaving a partially callable fiber.
- Cleanup continues after individual cleanup failures and reports an aggregate failure only after remaining cleanup settles.
- Async disposal is explicit; Rust `Drop` is not a substitute.

### Services and realms

- A required missing service prevents partial activation; an optional lookup does not change activation state.
- Removing a provider invalidates its old implementation/handle. Restoring it reactivates consumers against the new generation.
- Providers are visible only in the same realm. Identically named services in isolated realms are invisible to one another unless the realm is explicitly shared.

### Events

Listener ownership ends with scope disposal. Registration order is stable, except `prepend` explicitly moves a listener ahead of ordinary registrations. The event modes retain Cordis semantics:

| Mode | M0 rule |
|---|---|
| `emit` | Dispatches in stable listener registration order. |
| `parallel` | Starts eligible listeners without imposing completion order; result aggregation follows Cordis. |
| `serial` | Invokes listeners one at a time and observes Cordis serial-bail behavior. |
| `bail` | Uses Cordis synchronous bail behavior. |
| `waterfall` | Advances only when the current listener calls `next`; short-circuit and wrapping follow Cordis. |

Scoped filters, self-removal, owner disposal during dispatch, and `prepend` are observable cases. The trace records dispatch and listener ownership, not host callback internals.

### Loader

`loader` fixtures remain part of the shared schema to reserve the future contract, including `config-committed` and `config-rolled-back`. M0 does **not** execute them: the TypeScript oracle returns `UNSUPPORTED_SCENARIO`, and the Rust runner remains `NOT_IMPLEMENTED`. Later Loader work must validate candidates detached, preserve the last known-good tree on failure, and report rollback failures rather than masking them.

## Deterministic limitations

M0 intentionally does not compare:

- wall-clock timing, scheduling interleavings, or the relative completion order of independent top-level cleanup;
- stack traces, source paths, host error classes, memory addresses, object identity, or generated runtime IDs;
- JavaScript `Proxy`, prototype, `this` binding, declaration-merging, or callback implementation details;
- the ordering of parallel listener completion beyond any Cordis-guaranteed aggregate result;
- Loader execution, arbitrary JavaScript configuration evaluation, cross-runtime handles, or native/WASM behavior.

Those omissions are not permission to weaken lifecycle, service-realm, or Cordis event semantics. If a future fixture must rely on one, first make a stable, language-neutral observation and version this contract.

## Harness patch ledger — `harness-patch-ledger/v1`

| Patch contract | M0 status | Rationale |
|---|---|---|
| Reentrant disposal | normative baseline | Multiple disposal requests must converge on one cleanup process. |
| Initialization-failure rollback | normative baseline | A failed start cannot retain partially registered resources. |
| Async cleanup quiescence | normative baseline | Parent completion waits for child and async cleanup settlement. |
| Unload-time effect escape rejection | normative baseline | Unloading cannot register an effect that outlives the scope. |
| Parent/child fiber release order | normative baseline | Nested children are cleaned before the parent, with reverse nested registration order. |
| Lazy config resolution | recorded; Loader unsupported in M0 | It is a Loader semantic, preserved as an anchor without pretending M0 executes it. |
| Loader/Include/Group transactional update and rollback | recorded; Loader unsupported in M0 | Future Loader conformance must preserve last good state and expose rollback failure. |

Changing a ledger row's contract, source revision, or status requires increasing the ledger version and recording the reason in the change that updates it.

## Commands

Run from the repository root with a single fixture path:

```sh
bun run oracle/run.ts fixtures/conformance/<fixture>.json
cargo run -p tessivum-core --bin conformance-runner -- fixtures/conformance/<fixture>.json
```

The first command is the M0 behavioral oracle for supported lifecycle, service, and event scenarios. The second validates the same schema and then returns `NOT_IMPLEMENTED`. Do not treat an M0 Loader result as a pass: it is intentionally `UNSUPPORTED_SCENARIO`.
