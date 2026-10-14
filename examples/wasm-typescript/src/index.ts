type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

type Phase = "init" | "call" | "event" | "update" | "stop";
type ErrorPhase = Phase | "input" | "host";

type Request = {
  requestId: string;
  context: JsonValue;
  payload: JsonValue;
};

type GuestError = {
  code: string;
  message: string;
  phase: ErrorPhase;
  details?: JsonValue;
};

type Response =
  | { requestId: string; result: JsonValue }
  | { requestId: string; error: GuestError };

type Outcome = { result: JsonValue } | { error: GuestError };
type HostReply = { result: JsonValue } | { error: GuestError };
type CapabilityName =
  | "cordis.log"
  | "cordis.config.get"
  | "cordis.service.call"
  | "cordis.event.emit"
  | "cordis.event.subscribe"
  | "cordis.registration.dispose"
  | "cordis.kv.get"
  | "cordis.kv.set";

const hostFunctions = Host.getFunctions();
let matrix = false;

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasOwn(value: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function isRequest(value: unknown): value is Request {
  if (!isObject(value)) return false;
  const keys = Object.keys(value);
  return (
    keys.length === 3 &&
    hasOwn(value, "requestId") &&
    hasOwn(value, "context") &&
    hasOwn(value, "payload") &&
    typeof value.requestId === "string"
  );
}

function isGuestError(value: unknown): value is GuestError {
  if (!isObject(value)) return false;
  const keys = Object.keys(value);
  const phase = value.phase;
  return (
    (keys.length === 3 || (keys.length === 4 && hasOwn(value, "details"))) &&
    hasOwn(value, "code") &&
    hasOwn(value, "message") &&
    hasOwn(value, "phase") &&
    typeof value.code === "string" &&
    typeof value.message === "string" &&
    (phase === "init" ||
      phase === "call" ||
      phase === "event" ||
      phase === "update" ||
      phase === "stop" ||
      phase === "input" ||
      phase === "host")
  );
}

function output(response: Response): void {
  Host.outputString(JSON.stringify(response));
}

function invalidRequest(phase: Phase, requestId = ""): null {
  output({
    requestId,
    error: {
      code: "invalid_request",
      message: "invalid request envelope",
      phase,
    },
  });
  return null;
}

function request(phase: Phase): Request | null {
  let value: unknown;
  try {
    value = JSON.parse(Host.inputString());
  } catch {
    return invalidRequest(phase);
  }
  if (!isRequest(value)) {
    const requestId =
      isObject(value) && typeof value.requestId === "string" ? value.requestId : "";
    return invalidRequest(phase, requestId);
  }
  return value;
}

function dispatch(phase: Phase, operation: (input: Request) => Outcome): void {
  const input = request(phase);
  if (input === null) return;
  output({ requestId: input.requestId, ...operation(input) });
}

function error(code: string, message: string, phase: Phase): Outcome {
  return { error: { code, message, phase } };
}

function invokeHost(name: CapabilityName, payload: JsonValue): HostReply {
  const input = Memory.fromString(JSON.stringify(payload));
  let responseMemory: MemoryHandle | undefined;
  try {
    // build.mjs links compiler-only identifier keys to the exact dotted ABI names.
    const hostFunction = hostFunctions[name.replace(/\./g, "_") as CapabilityName];
    responseMemory = Memory.find(hostFunction(input.offset));
    if (!responseMemory) throw new Error(`${name} returned an invalid memory handle`);
    const response: unknown = JSON.parse(responseMemory.readString());
    if (!isObject(response) || Object.keys(response).length !== 1) {
      throw new Error(`${name} returned an invalid response envelope`);
    }
    if (hasOwn(response, "result")) return { result: response.result as JsonValue };
    if (hasOwn(response, "error") && isGuestError(response.error)) {
      return { error: response.error };
    }
    throw new Error(`${name} returned an invalid response envelope`);
  } finally {
    try {
      responseMemory?.free();
    } finally {
      input.free();
    }
  }
}

function lifecycle(phase: Phase): GuestError | undefined {
  if (!matrix) return undefined;
  const reply = invokeHost("cordis.log", {
    level: "info",
    message: `lifecycle:${phase}`,
  });
  return "error" in reply ? { ...reply.error, phase } : undefined;
}

function capability(payload: JsonValue): Outcome {
  if (!isObject(payload) || typeof payload.capability !== "string") {
    return error("invalid_operation", "missing capability", "call");
  }

  let name: CapabilityName;
  let requestPayload: JsonValue;
  switch (payload.capability) {
    case "cordis.log":
      name = "cordis.log";
      requestPayload = { level: "info", message: "matrix-log" };
      break;
    case "cordis.config.get":
      name = "cordis.config.get";
      requestPayload = { key: "theme" };
      break;
    case "cordis.service.call":
      name = "cordis.service.call";
      requestPayload = {
        service: "math",
        method: "increment",
        payload: { value: 41 },
      };
      break;
    case "cordis.event.emit":
      name = "cordis.event.emit";
      requestPayload = { topic: "matrix.notice", payload: { value: 7 } };
      break;
    case "cordis.event.subscribe":
      name = "cordis.event.subscribe";
      requestPayload = { topic: "matrix.notice" };
      break;
    case "cordis.registration.dispose":
      name = "cordis.registration.dispose";
      requestPayload = { id: "sub-1" };
      break;
    case "cordis.kv.get":
      name = "cordis.kv.get";
      requestPayload = { key: "matrix-key" };
      break;
    case "cordis.kv.set":
      name = "cordis.kv.set";
      requestPayload = { key: "matrix-key", value: 9 };
      break;
    default:
      return error("invalid_operation", "unknown capability", "call");
  }

  const reply = invokeHost(name, requestPayload);
  return {
    result:
      "result" in reply
        ? { ok: true, value: reply.result }
        : { ok: false, error: reply.error },
  };
}

function byteCount(payload: Record<string, unknown>, fallback: number): number {
  const bytes = payload.bytes;
  return typeof bytes === "number" && Number.isSafeInteger(bytes) && bytes >= 0
    ? bytes
    : fallback;
}

function call(input: Request): Outcome {
  const lifecycleError = lifecycle("call");
  if (lifecycleError) return { error: lifecycleError };

  const payload = isObject(input.payload) ? input.payload : undefined;
  switch (payload?.op) {
    case "inspect":
      return {
        result: {
          requestId: input.requestId,
          context: input.context,
          payload: input.payload,
        },
      };
    case "capability":
      return capability(input.payload);
    case "reject":
      return error("matrix_rejected", "deliberate guest rejection", "call");
    case "trap":
      throw new Error("deliberate guest trap");
    case "wasi-clock":
      return { result: { timestamp: Date.now() } };
    case "network": {
      const url =
        typeof payload.url === "string" ? payload.url : "http://127.0.0.1:9/";
      Http.request({ url, headers: {}, method: "GET" });
      return { result: { networkAllowed: true } };
    }
    case "spin": {
      if (payload.signalReady === true) {
        const reply = invokeHost("cordis.log", {
          level: "info",
          message: "spin-ready",
        });
        if ("error" in reply) return { error: reply.error };
      }
      let counter = 0;
      for (;;) counter = (counter + 1) >>> 0;
    }
    case "allocate": {
      const allocation = new Uint8Array(byteCount(payload, 16 * 1024 * 1024));
      for (let page = 0; page < allocation.length; page += 64 * 1024) {
        allocation[page] = 1;
      }
      return { result: { allocated: allocation.length } };
    }
    case "output":
      return { result: { data: "x".repeat(byteCount(payload, 16 * 1024)) } };
    default:
      return { result: { echo: input.payload } };
  }
}

export function cordis_init(): void {
  dispatch("init", (input) => {
    matrix = isObject(input.payload) && input.payload.matrix === true;
    const lifecycleError = lifecycle("init");
    return lifecycleError
      ? { error: lifecycleError }
      : { result: { abi: "cordis.plugin/v1", initialized: true } };
  });
}

export function cordis_call(): void {
  const input = request("call");
  if (input === null) return;
  const op = isObject(input.payload) ? input.payload.op : undefined;
  if (op === "malformed") {
    Host.outputString("{not-json");
  } else if (op === "unknown-field") {
    Host.outputString(JSON.stringify({ requestId: input.requestId, result: null, ok: true }));
  } else if (op === "wrong-id") {
    Host.outputString(JSON.stringify({ requestId: "wrong", result: null }));
  } else if (op === "both-results") {
    Host.outputString(
      JSON.stringify({
        requestId: input.requestId,
        result: null,
        error: { code: "bad", message: "bad", phase: "call" },
      }),
    );
  } else {
    output({ requestId: input.requestId, ...call(input) });
  }
}

export function cordis_event(): void {
  dispatch("event", () => {
    const lifecycleError = lifecycle("event");
    return lifecycleError ? { error: lifecycleError } : { result: { accepted: true } };
  });
}

export function cordis_update(): void {
  dispatch("update", () => {
    const lifecycleError = lifecycle("update");
    return lifecycleError ? { error: lifecycleError } : { result: { updated: true } };
  });
}

export function cordis_stop(): void {
  dispatch("stop", () => {
    const lifecycleError = lifecycle("stop");
    if (lifecycleError) return { error: lifecycleError };
    matrix = false;
    return { result: { stopped: true } };
  });
}
