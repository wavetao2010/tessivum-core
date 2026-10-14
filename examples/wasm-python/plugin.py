# Executed by the pinned MicroPython runtime embedded in guest.c.

PHASES = ("init", "call", "event", "update", "stop", "input", "host")
MATRIX = False


def exact_keys(value, names):
    if not isinstance(value, dict) or len(value) != len(names):
        return False
    for name in names:
        if name not in value:
            return False
    return True


def skip_space(value, index):
    while index < len(value) and value[index] in " \t\r\n":
        index += 1
    return index


def string_end(value, index):
    index += 1
    while index < len(value):
        char = value[index]
        if char == '"':
            return index + 1
        if ord(char) < 32:
            raise ValueError("invalid JSON string")
        if char == "\\":
            index += 1
            if index >= len(value) or value[index] not in '"\\/bfnrtu':
                raise ValueError("invalid JSON escape")
            if value[index] == "u":
                for _ in range(4):
                    index += 1
                    if index >= len(value) or value[index] not in "0123456789abcdefABCDEF":
                        raise ValueError("invalid JSON unicode escape")
        index += 1
    raise ValueError("unterminated JSON string")


def number_end(value, index):
    if value[index] == "-":
        index += 1
        if index == len(value):
            raise ValueError("invalid JSON number")
    if value[index] == "0":
        index += 1
        if index < len(value) and "0" <= value[index] <= "9":
            raise ValueError("invalid JSON number")
    elif "1" <= value[index] <= "9":
        while index < len(value) and "0" <= value[index] <= "9":
            index += 1
    else:
        raise ValueError("invalid JSON number")
    if index < len(value) and value[index] == ".":
        index += 1
        if index == len(value) or not "0" <= value[index] <= "9":
            raise ValueError("invalid JSON number")
        while index < len(value) and "0" <= value[index] <= "9":
            index += 1
    if index < len(value) and value[index] in "eE":
        index += 1
        if index < len(value) and value[index] in "+-":
            index += 1
        if index == len(value) or not "0" <= value[index] <= "9":
            raise ValueError("invalid JSON number")
        while index < len(value) and "0" <= value[index] <= "9":
            index += 1
    return index


def value_end(value, index):
    index = skip_space(value, index)
    if index == len(value):
        raise ValueError("missing JSON value")
    char = value[index]
    if char == '"':
        return string_end(value, index)
    if char == "{":
        index = skip_space(value, index + 1)
        if index < len(value) and value[index] == "}":
            return index + 1
        while True:
            if index == len(value) or value[index] != '"':
                raise ValueError("invalid JSON object key")
            index = skip_space(value, string_end(value, index))
            if index == len(value) or value[index] != ":":
                raise ValueError("missing JSON colon")
            index = skip_space(value, value_end(value, index + 1))
            if index < len(value) and value[index] == "}":
                return index + 1
            if index == len(value) or value[index] != ",":
                raise ValueError("missing JSON comma")
            index = skip_space(value, index + 1)
    if char == "[":
        index = skip_space(value, index + 1)
        if index < len(value) and value[index] == "]":
            return index + 1
        while True:
            index = skip_space(value, value_end(value, index))
            if index < len(value) and value[index] == "]":
                return index + 1
            if index == len(value) or value[index] != ",":
                raise ValueError("missing JSON comma")
            index = skip_space(value, index + 1)
    for literal in ("true", "false", "null"):
        if value[index : index + len(literal)] == literal:
            return index + len(literal)
    if char == "-" or "0" <= char <= "9":
        return number_end(value, index)
    raise ValueError("invalid JSON value")


def parse_json(value):
    if skip_space(value, value_end(value, 0)) != len(value):
        raise ValueError("trailing JSON data")
    # MicroPython decodes UTF-16 escapes independently; normalize surrogate pairs.
    parts = []
    start = index = 0
    while index < len(value):
        if value[index] != "\\":
            index += 1
            continue
        if value[index + 1] != "u":
            index += 2
            continue
        scalar = int(value[index + 2 : index + 6], 16)
        end = index + 6
        if 0xD800 <= scalar <= 0xDBFF:
            if value[end : end + 2] != "\\u" or end + 6 > len(value):
                raise ValueError("unpaired JSON surrogate")
            low = int(value[end + 2 : end + 6], 16)
            if not 0xDC00 <= low <= 0xDFFF:
                raise ValueError("unpaired JSON surrogate")
            scalar = 0x10000 + ((scalar - 0xD800) << 10) + low - 0xDC00
            end += 6
            parts.append(value[start:index])
            parts.append(chr(scalar))
            start = end
        elif 0xDC00 <= scalar <= 0xDFFF:
            raise ValueError("unpaired JSON surrogate")
        index = end
    if parts:
        parts.append(value[start:])
        value = "".join(parts)
    return json.loads(value)


def output(value):
    _output(json.dumps(value))


def guest_error(code, message, phase):
    return {"code": code, "message": message, "phase": phase}


def is_guest_error(value):
    if not exact_keys(value, ("code", "message", "phase")) and not exact_keys(
        value, ("code", "message", "phase", "details")
    ):
        return False
    return (
        isinstance(value["code"], str)
        and isinstance(value["message"], str)
        and isinstance(value["phase"], str)
        and value["phase"] in PHASES
    )


def response(request_id, body):
    body["requestId"] = request_id
    output(body)


def read_request(phase):
    try:
        value = parse_json(_input())
    except Exception:
        output(
            {
                "requestId": "",
                "error": guest_error(
                    "invalid_request", "invalid request envelope", phase
                ),
            }
        )
        return None
    request_id = value.get("requestId") if isinstance(value, dict) else None
    if (
        not exact_keys(value, ("requestId", "context", "payload"))
        or not isinstance(request_id, str)
    ):
        output(
            {
                "requestId": request_id if isinstance(request_id, str) else "",
                "error": guest_error(
                    "invalid_request", "invalid request envelope", phase
                ),
            }
        )
        return None
    return value


def invoke_host(name, payload):
    reply = parse_json(_host(name, json.dumps(payload)))
    if not isinstance(reply, dict) or len(reply) != 1:
        raise RuntimeError(name + " returned an invalid response envelope")
    if exact_keys(reply, ("result",)):
        return reply
    if exact_keys(reply, ("error",)) and is_guest_error(reply["error"]):
        return reply
    raise RuntimeError(name + " returned an invalid response envelope")


def lifecycle(phase):
    if not MATRIX:
        return None
    reply = invoke_host(
        "cordis.log", {"level": "info", "message": "lifecycle:" + phase}
    )
    if "error" not in reply:
        return None
    error = dict(reply["error"])
    error["phase"] = phase
    return error


def capability(payload):
    if not isinstance(payload, dict) or not isinstance(
        payload.get("capability"), str
    ):
        return {
            "error": guest_error(
                "invalid_operation", "missing capability", "call"
            )
        }
    name = payload["capability"]
    payloads = {
        "cordis.log": {"level": "info", "message": "matrix-log"},
        "cordis.config.get": {"key": "theme"},
        "cordis.service.call": {
            "service": "math",
            "method": "increment",
            "payload": {"value": 41},
        },
        "cordis.event.emit": {
            "topic": "matrix.notice",
            "payload": {"value": 7},
        },
        "cordis.event.subscribe": {"topic": "matrix.notice"},
        "cordis.registration.dispose": {"id": "sub-1"},
        "cordis.kv.get": {"key": "matrix-key"},
        "cordis.kv.set": {"key": "matrix-key", "value": 9},
    }
    if name not in payloads:
        return {
            "error": guest_error(
                "invalid_operation", "unknown capability", "call"
            )
        }
    reply = invoke_host(name, payloads[name])
    if "result" in reply:
        result = {"ok": True, "value": reply["result"]}
    else:
        result = {"ok": False, "error": reply["error"]}
    return {"result": result}


def byte_count(payload, fallback):
    value = payload.get("bytes")
    return (
        value
        if type(value) is int and 0 <= value <= 9007199254740991
        else fallback
    )


def call(request):
    lifecycle_error = lifecycle("call")
    if lifecycle_error is not None:
        return {"error": lifecycle_error}
    payload = request["payload"] if isinstance(request["payload"], dict) else None
    operation = payload.get("op") if payload is not None else None
    if operation == "inspect":
        return {
            "result": {
                "requestId": request["requestId"],
                "context": request["context"],
                "payload": request["payload"],
            }
        }
    if operation == "capability":
        return capability(request["payload"])
    if operation == "reject":
        return {
            "error": guest_error(
                "matrix_rejected", "deliberate guest rejection", "call"
            )
        }
    if operation == "trap":
        raise RuntimeError("deliberate guest trap")
    if operation == "wasi-clock":
        _wasi_clock()
    if operation == "network":
        url = payload.get("url")
        if not isinstance(url, str):
            url = "http://127.0.0.1:9/"
        _http(json.dumps({"url": url, "method": "GET", "headers": {}}))
        return {"result": {"networkAllowed": True}}
    if operation == "spin":
        if payload.get("signalReady") is True:
            reply = invoke_host(
                "cordis.log", {"level": "info", "message": "spin-ready"}
            )
            if "error" in reply:
                return {"error": reply["error"]}
        counter = 0
        while True:
            counter = (counter + 1) & 0xFFFFFFFF
    if operation == "allocate":
        size = byte_count(payload, 16 * 1024 * 1024)
        return {"result": {"allocated": _allocate(size)}}
    if operation == "gc":
        import gc
        checksum = 0
        for index in range(64):
            allocation = bytearray(1024)
            allocation[0] = index
            checksum += allocation[0]
        gc.collect()
        return {"result": {"checksum": checksum, "last": allocation[0]}}
    if operation == "output":
        return {"result": {"data": "x" * byte_count(payload, 16 * 1024)}}
    return {"result": {"echo": request["payload"]}}


def cordis_init():
    global MATRIX
    request = read_request("init")
    if request is None:
        return
    payload = request["payload"]
    MATRIX = isinstance(payload, dict) and payload.get("matrix") is True
    lifecycle_error = lifecycle("init")
    body = (
        {"error": lifecycle_error}
        if lifecycle_error is not None
        else {"result": {"abi": "cordis.plugin/v1", "initialized": True}}
    )
    response(request["requestId"], body)


def cordis_call():
    request = read_request("call")
    if request is None:
        return
    payload = request["payload"]
    operation = payload.get("op") if isinstance(payload, dict) else None
    if operation == "malformed":
        _output("{not-json")
    elif operation == "unknown-field":
        output(
            {"requestId": request["requestId"], "result": None, "ok": True}
        )
    elif operation == "wrong-id":
        output({"requestId": "wrong", "result": None})
    elif operation == "both-results":
        output(
            {
                "requestId": request["requestId"],
                "result": None,
                "error": guest_error("bad", "bad", "call"),
            }
        )
    else:
        response(request["requestId"], call(request))


def cordis_event():
    request = read_request("event")
    if request is None:
        return
    lifecycle_error = lifecycle("event")
    body = (
        {"error": lifecycle_error}
        if lifecycle_error is not None
        else {"result": {"accepted": True}}
    )
    response(request["requestId"], body)


def cordis_update():
    request = read_request("update")
    if request is None:
        return
    lifecycle_error = lifecycle("update")
    body = (
        {"error": lifecycle_error}
        if lifecycle_error is not None
        else {"result": {"updated": True}}
    )
    response(request["requestId"], body)


def cordis_stop():
    global MATRIX
    request = read_request("stop")
    if request is None:
        return
    lifecycle_error = lifecycle("stop")
    if lifecycle_error is not None:
        body = {"error": lifecycle_error}
    else:
        MATRIX = False
        body = {"result": {"stopped": True}}
    response(request["requestId"], body)
