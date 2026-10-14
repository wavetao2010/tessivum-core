package main

import (
	"encoding/json"
	"strings"

	pdk "github.com/extism/go-pdk"
)

const abi = "cordis.plugin/v1"

type request struct {
	RequestID string
	Context   json.RawMessage
	Payload   json.RawMessage
}

type guestError struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Phase   string          `json:"phase"`
	Details json.RawMessage `json:"details,omitempty"`
}

type response struct {
	RequestID string          `json:"requestId"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *guestError     `json:"error,omitempty"`
}

type outcome struct {
	Result json.RawMessage
	Error  *guestError
}

type hostReply struct {
	Result json.RawMessage
	Error  *guestError
}

type callPayload struct {
	Op          json.RawMessage `json:"op"`
	Capability  json.RawMessage `json:"capability"`
	URL         json.RawMessage `json:"url"`
	SignalReady json.RawMessage `json:"signalReady"`
	Bytes       json.RawMessage `json:"bytes"`
}

type logRequest struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type keyRequest struct {
	Key string `json:"key"`
}

var (
	matrix   bool
	spinSink uint64
)

//go:wasmimport extism:host/user cordis.log
func cordisLog(uint64) uint64

//go:wasmimport extism:host/user cordis.config.get
func cordisConfigGet(uint64) uint64

//go:wasmimport extism:host/user cordis.service.call
func cordisServiceCall(uint64) uint64

//go:wasmimport extism:host/user cordis.event.emit
func cordisEventEmit(uint64) uint64

//go:wasmimport extism:host/user cordis.event.subscribe
func cordisEventSubscribe(uint64) uint64

//go:wasmimport extism:host/user cordis.registration.dispose
func cordisRegistrationDispose(uint64) uint64

//go:wasmimport extism:host/user cordis.kv.get
func cordisKVGet(uint64) uint64

//go:wasmimport extism:host/user cordis.kv.set
func cordisKVSet(uint64) uint64

// This remains an import until build.sh links the trap-only WASI module.
//
//go:wasmimport wasi_snapshot_preview1 clock_time_get
func wasiClockTimeGet(uint32, uint64, uint32) uint32

func encode(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func decodeObject(data []byte) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return nil, false
	}
	return fields, true
}

func readRequest() (request, string, bool) {
	fields, ok := decodeObject(pdk.Input())
	if !ok {
		return request{}, "", false
	}
	requestID, ok := stringValue(fields["requestId"])
	if !ok {
		return request{}, "", false
	}
	if len(fields) != 3 || fields["context"] == nil || fields["payload"] == nil {
		return request{}, requestID, false
	}
	return request{RequestID: requestID, Context: fields["context"], Payload: fields["payload"]}, requestID, true
}

func success(value any) outcome {
	return outcome{Result: encode(value)}
}

func failure(code, message, phase string) outcome {
	return outcome{Error: &guestError{Code: code, Message: message, Phase: phase}}
}

func output(value any) {
	pdk.Output(encode(value))
}

func dispatch(phase string, operation func(request) outcome) int32 {
	input, requestID, ok := readRequest()
	if !ok {
		output(response{
			RequestID: requestID,
			Error: &guestError{
				Code:    "invalid_request",
				Message: "invalid request envelope",
				Phase:   phase,
			},
		})
		return 0
	}

	result := operation(input)
	if result.Error != nil {
		result.Error.Phase = phase
	}
	output(response{RequestID: input.RequestID, Result: result.Result, Error: result.Error})
	return 0
}

func decodeGuestError(data []byte) (*guestError, bool) {
	fields, ok := decodeObject(data)
	if !ok || (len(fields) != 3 && len(fields) != 4) || fields["code"] == nil || fields["message"] == nil || fields["phase"] == nil {
		return nil, false
	}
	if len(fields) == 4 && fields["details"] == nil {
		return nil, false
	}
	code, codeOK := stringValue(fields["code"])
	message, messageOK := stringValue(fields["message"])
	phase, phaseOK := stringValue(fields["phase"])
	if !codeOK || !messageOK || !phaseOK {
		return nil, false
	}
	switch phase {
	case "init", "call", "event", "update", "stop", "input", "host":
	default:
		return nil, false
	}
	return &guestError{Code: code, Message: message, Phase: phase, Details: fields["details"]}, true
}

func hostProtocolError() *guestError {
	return &guestError{Code: "host_protocol_error", Message: "invalid host capability response", Phase: "host"}
}

func invokeHost(function func(uint64) uint64, payload any) hostReply {
	input := pdk.AllocateBytes(encode(payload))
	defer input.Free()

	offset := function(input.Offset())
	if offset == 0 {
		return hostReply{Error: hostProtocolError()}
	}
	memory := pdk.FindMemory(offset)
	if memory.Length() == 0 {
		return hostReply{Error: hostProtocolError()}
	}
	data := memory.ReadBytes()
	memory.Free()

	fields, ok := decodeObject(data)
	if !ok || len(fields) != 1 {
		return hostReply{Error: hostProtocolError()}
	}
	if result := fields["result"]; result != nil {
		return hostReply{Result: result}
	}
	errorData := fields["error"]
	if errorData == nil {
		return hostReply{Error: hostProtocolError()}
	}
	error, ok := decodeGuestError(errorData)
	if !ok {
		return hostReply{Error: hostProtocolError()}
	}
	return hostReply{Error: error}
}

func lifecycle(phase string) *guestError {
	if !matrix {
		return nil
	}
	return invokeHost(func(offset uint64) uint64 { return cordisLog(offset) }, logRequest{Level: "info", Message: "lifecycle:" + phase}).Error
}

func stringValue(raw json.RawMessage) (string, bool) {
	var value *string
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return "", false
	}
	return *value, true
}

func boolValue(raw json.RawMessage) bool {
	var value bool
	return len(raw) != 0 && json.Unmarshal(raw, &value) == nil && value
}

func byteCount(raw json.RawMessage, fallback uint64) uint64 {
	var value uint64
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return fallback
	}
	return value
}

func parseCallPayload(raw json.RawMessage) callPayload {
	var payload callPayload
	_ = json.Unmarshal(raw, &payload)
	return payload
}

func capability(payload callPayload) outcome {
	name, ok := stringValue(payload.Capability)
	if !ok {
		return failure("invalid_operation", "missing capability", "call")
	}

	var reply hostReply
	switch name {
	case "cordis.log":
		reply = invokeHost(func(offset uint64) uint64 { return cordisLog(offset) }, logRequest{Level: "info", Message: "matrix-log"})
	case "cordis.config.get":
		reply = invokeHost(func(offset uint64) uint64 { return cordisConfigGet(offset) }, keyRequest{Key: "theme"})
	case "cordis.service.call":
		reply = invokeHost(func(offset uint64) uint64 { return cordisServiceCall(offset) }, struct {
			Service string `json:"service"`
			Method  string `json:"method"`
			Payload any    `json:"payload"`
		}{Service: "math", Method: "increment", Payload: struct {
			Value int `json:"value"`
		}{Value: 41}})
	case "cordis.event.emit":
		reply = invokeHost(func(offset uint64) uint64 { return cordisEventEmit(offset) }, struct {
			Topic   string `json:"topic"`
			Payload any    `json:"payload"`
		}{Topic: "matrix.notice", Payload: struct {
			Value int `json:"value"`
		}{Value: 7}})
	case "cordis.event.subscribe":
		reply = invokeHost(func(offset uint64) uint64 { return cordisEventSubscribe(offset) }, struct {
			Topic string `json:"topic"`
		}{Topic: "matrix.notice"})
	case "cordis.registration.dispose":
		reply = invokeHost(func(offset uint64) uint64 { return cordisRegistrationDispose(offset) }, struct {
			ID string `json:"id"`
		}{ID: "sub-1"})
	case "cordis.kv.get":
		reply = invokeHost(func(offset uint64) uint64 { return cordisKVGet(offset) }, keyRequest{Key: "matrix-key"})
	case "cordis.kv.set":
		reply = invokeHost(func(offset uint64) uint64 { return cordisKVSet(offset) }, struct {
			Key   string `json:"key"`
			Value int    `json:"value"`
		}{Key: "matrix-key", Value: 9})
	default:
		return failure("invalid_operation", "unknown capability", "call")
	}

	if reply.Error != nil {
		return success(struct {
			OK    bool        `json:"ok"`
			Error *guestError `json:"error"`
		}{OK: false, Error: reply.Error})
	}
	return success(struct {
		OK    bool            `json:"ok"`
		Value json.RawMessage `json:"value"`
	}{OK: true, Value: reply.Result})
}

func call(input request) outcome {
	if err := lifecycle("call"); err != nil {
		return outcome{Error: err}
	}

	payload := parseCallPayload(input.Payload)
	op, _ := stringValue(payload.Op)
	switch op {
	case "inspect":
		return success(struct {
			RequestID string          `json:"requestId"`
			Context   json.RawMessage `json:"context"`
			Payload   json.RawMessage `json:"payload"`
		}{RequestID: input.RequestID, Context: input.Context, Payload: input.Payload})
	case "capability":
		return capability(payload)
	case "reject":
		return failure("matrix_rejected", "deliberate guest rejection", "call")
	case "trap":
		panic("deliberate guest trap")
	case "wasi-clock":
		wasiClockTimeGet(0, 0, 0)
		return success(struct {
			Timestamp int `json:"timestamp"`
		}{Timestamp: 0})
	case "network":
		url, ok := stringValue(payload.URL)
		if !ok {
			url = "http://127.0.0.1:9/"
		}
		response := pdk.NewHTTPRequest(pdk.MethodGet, url).Send()
		memory := response.Memory()
		memory.Free()
		return success(struct {
			NetworkAllowed bool `json:"networkAllowed"`
		}{NetworkAllowed: true})
	case "spin":
		if boolValue(payload.SignalReady) {
			if reply := invokeHost(func(offset uint64) uint64 { return cordisLog(offset) }, logRequest{Level: "info", Message: "spin-ready"}); reply.Error != nil {
				return outcome{Error: reply.Error}
			}
		}
		for {
			spinSink++
		}
	case "allocate":
		// Use the PDK's linear-memory buffer; TinyGo's per-block GC marking
		// consumes the fuel budget before a large Go slice can be returned.
		size := byteCount(payload.Bytes, 16*1024*1024)
		if size > uint64(^uint32(0)>>1) {
			panic("allocation exceeds wasm32 int range")
		}
		buffer := pdk.Allocate(int(size))
		if size != 0 && buffer.Offset() == 0 {
			panic("allocation failed")
		}
		defer buffer.Free()
		for page := uint64(0); page < size; page += 64 * 1024 {
			cell := pdk.NewMemory(buffer.Offset()+page, 1)
			cell.Store([]byte{1})
		}
		return success(struct {
			Allocated uint64 `json:"allocated"`
		}{Allocated: buffer.Length()})
	case "output":
		return success(struct {
			Data string `json:"data"`
		}{Data: strings.Repeat("x", int(byteCount(payload.Bytes, 16*1024)))})
	default:
		return success(struct {
			Echo json.RawMessage `json:"echo"`
		}{Echo: input.Payload})
	}
}

//go:wasmexport cordis_init
func cordisInit() int32 {
	return dispatch("init", func(input request) outcome {
		var payload struct {
			Matrix json.RawMessage `json:"matrix"`
		}
		_ = json.Unmarshal(input.Payload, &payload)
		matrix = boolValue(payload.Matrix)
		if err := lifecycle("init"); err != nil {
			return outcome{Error: err}
		}
		return success(struct {
			ABI         string `json:"abi"`
			Initialized bool   `json:"initialized"`
		}{ABI: abi, Initialized: true})
	})
}

//go:wasmexport cordis_call
func cordisCall() int32 {
	input, requestID, ok := readRequest()
	if !ok {
		output(response{RequestID: requestID, Error: &guestError{Code: "invalid_request", Message: "invalid request envelope", Phase: "call"}})
		return 0
	}

	payload := parseCallPayload(input.Payload)
	op, _ := stringValue(payload.Op)
	switch op {
	case "malformed":
		pdk.OutputString("{not-json")
	case "unknown-field":
		pdk.OutputString(`{"requestId":` + string(encode(input.RequestID)) + `,"result":null,"ok":true}`)
	case "wrong-id":
		pdk.OutputString(`{"requestId":"wrong","result":null}`)
	case "both-results":
		pdk.OutputString(`{"requestId":` + string(encode(input.RequestID)) + `,"result":null,"error":{"code":"bad","message":"bad","phase":"call"}}`)
	default:
		result := call(input)
		if result.Error != nil {
			result.Error.Phase = "call"
		}
		output(response{RequestID: input.RequestID, Result: result.Result, Error: result.Error})
	}
	return 0
}

//go:wasmexport cordis_event
func cordisEvent() int32 {
	return dispatch("event", func(request) outcome {
		if err := lifecycle("event"); err != nil {
			return outcome{Error: err}
		}
		return success(struct {
			Accepted bool `json:"accepted"`
		}{Accepted: true})
	})
}

//go:wasmexport cordis_update
func cordisUpdate() int32 {
	return dispatch("update", func(request) outcome {
		if err := lifecycle("update"); err != nil {
			return outcome{Error: err}
		}
		return success(struct {
			Updated bool `json:"updated"`
		}{Updated: true})
	})
}

//go:wasmexport cordis_stop
func cordisStop() int32 {
	return dispatch("stop", func(request) outcome {
		if err := lifecycle("stop"); err != nil {
			return outcome{Error: err}
		}
		matrix = false
		return success(struct {
			Stopped bool `json:"stopped"`
		}{Stopped: true})
	})
}

func main() {}
