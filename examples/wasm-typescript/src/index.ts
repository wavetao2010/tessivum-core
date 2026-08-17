type Request = {
  requestId: string;
  context: Record<string, unknown>;
  payload: unknown;
};

type GuestError = {
  code: string;
  message: string;
  phase: "init" | "call" | "event" | "update" | "stop";
  details?: unknown;
};

type Response =
  | { requestId: string; ok: true; result: unknown }
  | { requestId: string; ok: false; error: GuestError };

let configuration: unknown = null;
let stopped = false;

function isRequest(value: unknown): value is Request {
  return (
    !!value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "requestId" in value &&
    typeof value.requestId === "string" &&
    "context" in value &&
    !!value.context &&
    typeof value.context === "object" &&
    !Array.isArray(value.context) &&
    "payload" in value
  );
}

function request(): Request {
  const value: unknown = JSON.parse(Host.inputString());
  if (!isRequest(value)) {
    throw new Error("input must be a cordis.plugin/v1 request envelope");
  }
  return value;
}

function output(response: Response): void {
  Host.outputString(JSON.stringify(response));
}

function failure(input: Request, phase: GuestError["phase"], error: unknown): void {
  output({
    requestId: input.requestId,
    ok: false,
    error: {
      code: "guest-failure",
      message: error instanceof Error ? error.message : String(error),
      phase,
    },
  });
}

function run(phase: GuestError["phase"], operation: (input: Request) => unknown): void {
  let input: Request | undefined;
  try {
    input = request();
    output({ requestId: input.requestId, ok: true, result: operation(input) });
  } catch (error) {
    if (input) {
      failure(input, phase, error);
      return;
    }
    throw error;
  }
}

export function cordis_init(): void {
  run("init", (input) => {
    configuration = input.payload;
    stopped = false;
    return { initialized: true, configuration };
  });
}

export function cordis_call(): void {
  run("call", (input) => {
    if (stopped) {
      throw new Error("plugin has stopped");
    }
    return { method: input.context.method ?? null, payload: input.payload };
  });
}

export function cordis_event(): void {
  run("event", (input) => {
    if (stopped) {
      throw new Error("plugin has stopped");
    }
    return { event: input.context.event ?? null, payload: input.payload };
  });
}

export function cordis_update(): void {
  run("update", (input) => {
    if (stopped) {
      throw new Error("plugin has stopped");
    }
    configuration = input.payload;
    return { updated: true, configuration };
  });
}

export function cordis_stop(): void {
  run("stop", () => {
    stopped = true;
    return { stopped: true };
  });
}
