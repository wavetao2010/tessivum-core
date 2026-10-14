#include <stddef.h>
#include <stdint.h>

#include "py/persistentcode.h"
#include "py/gc.h"
#include "py/objmodule.h"
#include "py/objstr.h"
#include "py/runtime.h"
#include "py/stackctrl.h"
#include "shared/runtime/gchelper.h"
#include "plugin_source.h"

#define IMPORT_ENV(name) __attribute__((import_module("extism:host/env"), import_name(name)))
#define IMPORT_USER(name) __attribute__((import_module("extism:host/user"), import_name(name)))
#define EXPORT(name) __attribute__((export_name(name)))

IMPORT_ENV("input_length") uint64_t extism_input_length(void);
IMPORT_ENV("input_load_u8") uint8_t extism_input_load_u8(uint64_t offset);
IMPORT_ENV("length_unsafe") uint64_t extism_length(uint64_t handle);
IMPORT_ENV("load_u8") uint8_t extism_load_u8(uint64_t offset);
IMPORT_ENV("alloc") uint64_t extism_alloc(uint64_t length);
IMPORT_ENV("store_u8") void extism_store_u8(uint64_t offset, uint8_t value);
IMPORT_ENV("free") void extism_free(uint64_t handle);
IMPORT_ENV("output_set") void extism_output_set(uint64_t handle, uint64_t length);
IMPORT_ENV("http_request") uint64_t extism_http_request(uint64_t request, uint64_t body);

IMPORT_USER("cordis.log") uint64_t host_log(uint64_t request);
IMPORT_USER("cordis.config.get") uint64_t host_config_get(uint64_t request);
IMPORT_USER("cordis.service.call") uint64_t host_service_call(uint64_t request);
IMPORT_USER("cordis.event.emit") uint64_t host_event_emit(uint64_t request);
IMPORT_USER("cordis.event.subscribe") uint64_t host_event_subscribe(uint64_t request);
IMPORT_USER("cordis.registration.dispose") uint64_t host_registration_dispose(uint64_t request);
IMPORT_USER("cordis.kv.get") uint64_t host_kv_get(uint64_t request);
IMPORT_USER("cordis.kv.set") uint64_t host_kv_set(uint64_t request);

extern const mp_obj_module_t mp_module_json;

static uint8_t initial_heap[768 * 1024] __attribute__((aligned(16)));
static int runtime_started;
static int source_loaded;
// Hard WASM traps bypass Python unwinding; never re-enter that VM, including stop.
static volatile int executing;

static int string_equals(const byte *value, size_t length, const char *expected) {
    size_t i = 0;
    while (expected[i] != '\0') {
        if (i == length || value[i] != (byte)expected[i]) {
            return 0;
        }
        ++i;
    }
    return i == length;
}

static uint64_t copy_to_extism(const byte *data, size_t length) {
    uint64_t handle = extism_alloc(length);
    for (size_t i = 0; i < length; ++i) {
        extism_store_u8(handle + i, data[i]);
    }
    return handle;
}

static mp_obj_t copy_from_extism(uint64_t handle) {
    uint64_t length64 = extism_length(handle);
    if (length64 > SIZE_MAX) {
        mp_raise_type(&mp_type_MemoryError);
    }
    size_t length = (size_t)length64;
    byte *data = m_new(byte, length);
    for (size_t i = 0; i < length; ++i) {
        data[i] = extism_load_u8(handle + i);
    }
    mp_obj_t value = mp_obj_new_str((const char *)data, length);
    m_del(byte, data, length);
    return value;
}

static mp_obj_t py_input(void) {
    uint64_t length64 = extism_input_length();
    if (length64 > SIZE_MAX) {
        mp_raise_type(&mp_type_MemoryError);
    }
    size_t length = (size_t)length64;
    byte *data = m_new(byte, length);
    for (size_t i = 0; i < length; ++i) {
        data[i] = extism_input_load_u8(i);
    }
    mp_obj_t value = mp_obj_new_str((const char *)data, length);
    m_del(byte, data, length);
    return value;
}
static MP_DEFINE_CONST_FUN_OBJ_0(py_input_obj, py_input);

static mp_obj_t py_output(mp_obj_t value) {
    size_t length;
    const byte *data = (const byte *)mp_obj_str_get_data(value, &length);
    uint64_t handle = copy_to_extism(data, length);
    extism_output_set(handle, length);
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_1(py_output_obj, py_output);

static uint64_t invoke_host(const byte *name, size_t length, uint64_t request) {
    if (string_equals(name, length, "cordis.log")) {
        return host_log(request);
    }
    if (string_equals(name, length, "cordis.config.get")) {
        return host_config_get(request);
    }
    if (string_equals(name, length, "cordis.service.call")) {
        return host_service_call(request);
    }
    if (string_equals(name, length, "cordis.event.emit")) {
        return host_event_emit(request);
    }
    if (string_equals(name, length, "cordis.event.subscribe")) {
        return host_event_subscribe(request);
    }
    if (string_equals(name, length, "cordis.registration.dispose")) {
        return host_registration_dispose(request);
    }
    if (string_equals(name, length, "cordis.kv.get")) {
        return host_kv_get(request);
    }
    if (string_equals(name, length, "cordis.kv.set")) {
        return host_kv_set(request);
    }
    mp_raise_ValueError(MP_ERROR_TEXT("unknown Host function"));
}

static mp_obj_t py_host(mp_obj_t name_obj, mp_obj_t payload_obj) {
    size_t name_length;
    size_t payload_length;
    const byte *name = (const byte *)mp_obj_str_get_data(name_obj, &name_length);
    const byte *payload = (const byte *)mp_obj_str_get_data(payload_obj, &payload_length);
    uint64_t request = copy_to_extism(payload, payload_length);
    uint64_t reply = invoke_host(name, name_length, request);
    extism_free(request);
    mp_obj_t value = copy_from_extism(reply);
    extism_free(reply);
    return value;
}
static MP_DEFINE_CONST_FUN_OBJ_2(py_host_obj, py_host);

static mp_obj_t py_http(mp_obj_t request_obj) {
    size_t length;
    const byte *request_data = (const byte *)mp_obj_str_get_data(request_obj, &length);
    uint64_t request = copy_to_extism(request_data, length);
    uint64_t reply = extism_http_request(request, 0);
    extism_free(request);
    if (reply != 0) {
        extism_free(reply);
    }
    return mp_const_none;
}
static MP_DEFINE_CONST_FUN_OBJ_1(py_http_obj, py_http);

static mp_obj_t py_allocate(mp_obj_t size_obj) {
    mp_int_t requested = mp_obj_get_int(size_obj);
    if (requested < 0) {
        mp_raise_ValueError(MP_ERROR_TEXT("negative allocation"));
    }
    uint64_t handle = extism_alloc((uint64_t)requested);
    if (requested && !handle) {
        mp_raise_type(&mp_type_MemoryError);
    }
    for (uint64_t offset = 0; offset < (uint64_t)requested; offset += 65536) {
        extism_store_u8(handle + offset, 1);
    }
    extism_free(handle);
    return mp_obj_new_int(requested);
}
static MP_DEFINE_CONST_FUN_OBJ_1(py_allocate_obj, py_allocate);

static mp_obj_t py_wasi_clock(void) {
    // This no-WASI port denies the authority locally with WASI NOTCAPABLE.
    mp_raise_OSError(76);
}
static MP_DEFINE_CONST_FUN_OBJ_0(py_wasi_clock_obj, py_wasi_clock);

static mp_obj_t denied_open(size_t n_args, const mp_obj_t *args, mp_map_t *kwargs) {
    (void)n_args;
    (void)args;
    (void)kwargs;
    mp_raise_OSError(76);
}
MP_DEFINE_CONST_FUN_OBJ_KW(mp_builtin_open_obj, 1, denied_open);

static void store_global(const char *name, const mp_obj_t value) {
    mp_store_global(qstr_from_str(name), value);
}

static void start_runtime(void *stack_top) {
    mp_stack_set_top(stack_top);
    if (runtime_started) {
        return;
    }
    gc_init(initial_heap, initial_heap + sizeof(initial_heap));
    mp_init();
    store_global("json", MP_OBJ_FROM_PTR(&mp_module_json));
    store_global("_input", MP_OBJ_FROM_PTR(&py_input_obj));
    store_global("_output", MP_OBJ_FROM_PTR(&py_output_obj));
    store_global("_host", MP_OBJ_FROM_PTR(&py_host_obj));
    store_global("_http", MP_OBJ_FROM_PTR(&py_http_obj));
    store_global("_allocate", MP_OBJ_FROM_PTR(&py_allocate_obj));
    store_global("_wasi_clock", MP_OBJ_FROM_PTR(&py_wasi_clock_obj));
    runtime_started = 1;
}

static void load_source(void) {
    if (source_loaded) {
        return;
    }
    mp_compiled_module_t compiled;
    compiled.context = m_new_obj(mp_module_context_t);
    compiled.context->module.base.type = &mp_type_module;
    compiled.context->module.globals = mp_globals_get();
    mp_raw_code_load_mem(plugin_source, plugin_source_len, &compiled);
    mp_obj_t module = mp_make_function_from_proto_fun(compiled.rc, compiled.context, NULL);
    mp_call_function_0(module);
    source_loaded = 1;
}

static int32_t call_python(const char *name, void *stack_top) {
    if (executing) {
        __builtin_trap();
    }
    executing = 1;
    start_runtime(stack_top);
    nlr_buf_t nlr;
    if (nlr_push(&nlr) == 0) {
        load_source();
        mp_obj_t function = mp_load_global(qstr_from_str(name));
        mp_call_function_0(function);
        nlr_pop();
        executing = 0;
        return 0;
    }
    __builtin_trap();
}

EXPORT("cordis_init") int32_t cordis_init(void) {
    int stack_top;
    return call_python("cordis_init", &stack_top);
}
EXPORT("cordis_call") int32_t cordis_call(void) {
    int stack_top;
    return call_python("cordis_call", &stack_top);
}
EXPORT("cordis_event") int32_t cordis_event(void) {
    int stack_top;
    return call_python("cordis_event", &stack_top);
}
EXPORT("cordis_update") int32_t cordis_update(void) {
    int stack_top;
    return call_python("cordis_update", &stack_top);
}
EXPORT("cordis_stop") int32_t cordis_stop(void) {
    int stack_top;
    return call_python("cordis_stop", &stack_top);
}

void gc_collect(void) {
    gc_collect_start();
    gc_helper_collect_regs_and_stack();
    gc_collect_end();
}

void mp_hal_stdout_tx_strn_cooked(const char *str, size_t length) {
    (void)str;
    (void)length;
    __builtin_trap();
}

void nlr_jump_fail(void *value) {
    (void)value;
    __builtin_trap();
}

#ifndef NDEBUG
void __assert_func(const char *file, int line, const char *function, const char *expression) {
    (void)file;
    (void)line;
    (void)function;
    (void)expression;
    __builtin_trap();
}
#endif
