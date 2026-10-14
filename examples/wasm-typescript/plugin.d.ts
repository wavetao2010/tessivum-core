declare module "main" {
  export function cordis_init(): I32;
  export function cordis_call(): I32;
  export function cordis_event(): I32;
  export function cordis_update(): I32;
  export function cordis_stop(): I32;
}

declare module "extism:host" {
  interface user {
    "cordis.log"(ptr: I64): I64;
    "cordis.config.get"(ptr: I64): I64;
    "cordis.service.call"(ptr: I64): I64;
    "cordis.event.emit"(ptr: I64): I64;
    "cordis.event.subscribe"(ptr: I64): I64;
    "cordis.registration.dispose"(ptr: I64): I64;
    "cordis.kv.get"(ptr: I64): I64;
    "cordis.kv.set"(ptr: I64): I64;
  }
}
