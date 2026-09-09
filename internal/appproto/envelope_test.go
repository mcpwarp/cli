package appproto

import (
	"encoding/json"
	"testing"
)

func mustEncode(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestEncodeRegister(t *testing.T) {
	body := EncodeRegister([]RegisterService{{Name: "a", Kind: "stdio"}, {Name: "b", Kind: "http"}})
	raw := mustEncode(t, body)

	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("re-decoding our own register body should never error: %v", err)
	}
	// register is outbound-only; decoding it back finds an op we don't
	// recognize as inbound, so it comes back as OpUnknown for the caller to
	// warn-log and ignore — sanity check the shape is otherwise well-formed
	// by inspecting the raw JSON instead.
	if got == nil || got.Op != OpUnknown || got.UnknownOp != OpRegister {
		t.Fatalf("register is not a known inbound op, want OpUnknown/register, got %+v", got)
	}
	var wrapper struct {
		Mcpwarp struct {
			V        int    `json:"v"`
			Op       string `json:"op"`
			Services []struct {
				Name string `json:"name"`
				Kind string `json:"kind"`
			} `json:"services"`
		} `json:"mcpwarp"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if wrapper.Mcpwarp.V != 1 || wrapper.Mcpwarp.Op != "register" {
		t.Fatalf("unexpected envelope: %+v", wrapper.Mcpwarp)
	}
	if len(wrapper.Mcpwarp.Services) != 2 || wrapper.Mcpwarp.Services[0].Name != "a" || wrapper.Mcpwarp.Services[0].Kind != "stdio" {
		t.Fatalf("unexpected services: %+v", wrapper.Mcpwarp.Services)
	}
}

func TestEncodeUnregister(t *testing.T) {
	body := EncodeUnregister([]UnregisterService{{Name: "a"}})
	raw := mustEncode(t, body)
	var wrapper struct {
		Mcpwarp struct {
			Op       string `json:"op"`
			Services []struct {
				Name string `json:"name"`
			} `json:"services"`
		} `json:"mcpwarp"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if wrapper.Mcpwarp.Op != "unregister" || len(wrapper.Mcpwarp.Services) != 1 || wrapper.Mcpwarp.Services[0].Name != "a" {
		t.Fatalf("unexpected envelope: %+v", wrapper.Mcpwarp)
	}
}

func TestDecodeRegistered(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"a","id":"id1","created":true}],"errors":[{"name":"b","code":"CONFLICT","message":"nope"}]}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got == nil || got.Op != OpRegistered {
		t.Fatalf("unexpected result: %+v", got)
	}
	if len(got.Services) != 1 || got.Services[0].Name != "a" || got.Services[0].ID != "id1" || got.Services[0].URL != "" || !got.Services[0].Created {
		t.Fatalf("unexpected services: %+v", got.Services)
	}
	if len(got.Errors) != 1 || got.Errors[0].Code != "CONFLICT" {
		t.Fatalf("unexpected errors: %+v", got.Errors)
	}
}

func TestDecodeRegisteredDefaultsURLAndErrors(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"registered","services":[{"name":"a","id":"id1","created":false}]}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Services[0].URL != "" {
		t.Fatalf("expected default empty url, got %q", got.Services[0].URL)
	}
	if got.Errors == nil || len(got.Errors) != 0 {
		t.Fatalf("expected empty (non-nil) errors slice, got %+v", got.Errors)
	}
}

func TestDecodeUnregistered(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":"a","id":"id1"}]}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Op != OpUnregistered || len(got.UnregisteredServices) != 1 || got.UnregisteredServices[0].ID != "id1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestDecodeDisable(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"disable","id":"id1","reason":"quota"}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Op != OpDisable || got.DisableID != "id1" || got.DisableReason != "quota" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestDecodeEnable(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"enable","id":"id1","name":"a"}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Op != OpEnable || got.EnableID != "id1" || got.EnableName != "a" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestDecodeError(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"error","code":"OVERLOADED","message":"slow down"}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Op != OpError || got.ErrorCode != "OVERLOADED" || got.ErrorMessage != "slow down" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestDecodeUnknownOpIgnored(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":1,"op":"something-new","foo":"bar"}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("unknown op must be ignored, not errored: %v", err)
	}
	if got == nil || got.Op != OpUnknown || got.UnknownOp != "something-new" || got.UnknownV != 1 {
		t.Fatalf("unknown op must decode to OpUnknown carrying the raw op/v, got %+v", got)
	}
}

func TestDecodeUnsupportedVersionIgnored(t *testing.T) {
	raw := json.RawMessage(`{"mcpwarp":{"v":2,"op":"disable","id":"id1","reason":"x"}}`)
	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("unsupported version must be ignored, not errored: %v", err)
	}
	if got == nil || got.Op != OpUnknown || got.UnknownOp != "disable" || got.UnknownV != 2 {
		t.Fatalf("unsupported version must decode to OpUnknown carrying the raw op/v, got %+v", got)
	}
}

func TestDecodeNoEnvelopeIgnored(t *testing.T) {
	raw := json.RawMessage(`{"foo":"bar"}`)
	got, err := Decode(raw)
	if err != nil || got != nil {
		t.Fatalf("no envelope must be ignored: got=%+v err=%v", got, err)
	}
}

func TestDecodeBadShapeErrors(t *testing.T) {
	cases := []json.RawMessage{
		json.RawMessage(`{"mcpwarp":{"v":1,"op":"registered","services":"not-an-array"}}`),
		json.RawMessage(`{"mcpwarp":{"v":1,"op":"disable","reason":"missing id"}}`),
		json.RawMessage(`{"mcpwarp":{"v":1,"op":"enable","id":"id1"}}`),
		json.RawMessage(`{"mcpwarp":{"v":1,"op":"error","message":"missing code"}}`),
		json.RawMessage(`{"mcpwarp":{"v":1,"op":"unregistered","services":[{"name":1}]}}`),
	}
	for i, raw := range cases {
		got, err := Decode(raw)
		if err == nil {
			t.Fatalf("case %d: expected a DecodeError, got %+v", i, got)
		}
		if _, ok := err.(*DecodeError); !ok {
			t.Fatalf("case %d: expected *DecodeError, got %T: %v", i, err, err)
		}
	}
}
