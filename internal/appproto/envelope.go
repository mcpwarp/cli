// Package appproto implements the {"mcpwarp":{"v":1,"op":...}} app-channel
// envelope carried over wsmixer's stream-0 SendApp/OnApp (DESIGN.md §8).
// Shapes are taken from the Go wire structs in mcpwarp-saas's
// apps/tunnel/internal/agentconn/agentconn.go, ported behaviourally from
// mcpwarp-cli's src/tunnel/envelope.ts.
package appproto

import (
	"encoding/json"
	"fmt"
)

// ProtocolVersion is the only "v" this CLI speaks.
const ProtocolVersion = 1

// RegisterService is one entry of an outbound "register" op.
type RegisterService struct {
	Name string
	Kind string
}

// UnregisterService is one entry of an outbound "unregister" op.
type UnregisterService struct {
	Name string
}

// RegisteredService is one entry of an inbound "registered" op.
type RegisteredService struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	URL     string `json:"url"`
	Created bool   `json:"created"`
}

// ServiceError is one entry of an inbound "registered" op's errors list.
type ServiceError struct {
	Name    string `json:"name"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// UnregisteredService is one entry of an inbound "unregistered" op.
type UnregisteredService struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// Op names, inbound and outbound.
const (
	OpRegister     = "register"
	OpUnregister   = "unregister"
	OpRegistered   = "registered"
	OpUnregistered = "unregistered"
	OpDisable      = "disable"
	OpEnable       = "enable"
	OpError        = "error"
	// OpUnknown is Inbound.Op for a message whose op Decode doesn't
	// recognize, or whose v isn't ProtocolVersion — DESIGN.md §8's
	// "ignored-with-log": the caller should warn-log it (UnknownOp/UnknownV
	// carry what was actually seen) and otherwise ignore it, never treat it
	// as a DecodeError.
	OpUnknown = "unknown"
)

// Inbound is the decoded shape of one inbound envelope, discriminated by Op.
// Only the fields for that Op are populated.
type Inbound struct {
	Op string

	// "registered"
	Services []RegisteredService
	Errors   []ServiceError

	// "unregistered"
	UnregisteredServices []UnregisteredService

	// "disable"
	DisableID     string
	DisableReason string

	// "enable"
	EnableID   string
	EnableName string

	// "error"
	ErrorCode    string
	ErrorMessage string

	// "unknown": the op/v actually seen, for the caller's warn log.
	UnknownOp string
	UnknownV  int
}

var knownOps = map[string]bool{
	OpRegistered:   true,
	OpUnregistered: true,
	OpDisable:      true,
	OpEnable:       true,
	OpError:        true,
}

// DecodeError is returned by Decode when a recognized op's body fails to
// match its expected shape — a genuine protocol violation, unlike an
// unknown op/version (which Decode ignores by returning (nil, nil)).
type DecodeError struct {
	Op      string
	Message string
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("invalid mcpwarp %s envelope: %s", e.Op, e.Message)
}

// envelopeWrapper is the {"mcpwarp": {...}} shape every app message carries.
type envelopeWrapper struct {
	Mcpwarp json.RawMessage `json:"mcpwarp"`
}

// opAndVersion peeks at just "v" and "op", the two fields every envelope
// shares regardless of op-specific payload.
type opAndVersion struct {
	V  int    `json:"v"`
	Op string `json:"op"`
}

// EncodeRegister builds the {"mcpwarp":{"v":1,"op":"register",...}} outbound
// body.
func EncodeRegister(services []RegisterService) map[string]any {
	list := make([]map[string]any, len(services))
	for i, s := range services {
		list[i] = map[string]any{"name": s.Name, "kind": s.Kind}
	}
	return map[string]any{
		"mcpwarp": map[string]any{
			"v":        ProtocolVersion,
			"op":       OpRegister,
			"services": list,
		},
	}
}

// EncodeUnregister builds the {"mcpwarp":{"v":1,"op":"unregister",...}}
// outbound body.
func EncodeUnregister(services []UnregisterService) map[string]any {
	list := make([]map[string]any, len(services))
	for i, s := range services {
		list[i] = map[string]any{"name": s.Name}
	}
	return map[string]any{
		"mcpwarp": map[string]any{
			"v":        ProtocolVersion,
			"op":       OpUnregister,
			"services": list,
		},
	}
}

// Decode decodes one inbound app-message body (wsmixer's OnApp payload).
// Returns (nil, nil) for a message with no mcpwarp envelope at all, or one
// that isn't even shaped like {v,op} — there's nothing worth logging about
// either. An unsupported version or an unrecognized op instead returns an
// *Inbound with Op == OpUnknown (UnknownOp/UnknownV carry what was actually
// seen) so the caller can warn-log it before ignoring it — neither is
// treated as fatal. A recognized op whose body fails to match its expected
// shape returns a *DecodeError.
func Decode(raw json.RawMessage) (*Inbound, error) {
	var wrapper envelopeWrapper
	if err := json.Unmarshal(raw, &wrapper); err != nil || wrapper.Mcpwarp == nil {
		return nil, nil
	}

	var head opAndVersion
	if err := json.Unmarshal(wrapper.Mcpwarp, &head); err != nil {
		return nil, nil
	}
	if head.V != ProtocolVersion || !knownOps[head.Op] {
		return &Inbound{Op: OpUnknown, UnknownOp: head.Op, UnknownV: head.V}, nil
	}

	switch head.Op {
	case OpRegistered:
		// Pointers so a genuinely absent JSON key (zod's "required") is
		// distinguishable from an explicit zero value (e.g. created:false,
		// message:"") — Go's plain string/bool unmarshal can't tell those
		// apart otherwise.
		var body struct {
			Services []struct {
				Name    *string `json:"name"`
				ID      *string `json:"id"`
				URL     *string `json:"url"`
				Created *bool   `json:"created"`
			} `json:"services"`
			Errors []struct {
				Name    *string `json:"name"`
				Code    *string `json:"code"`
				Message *string `json:"message"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(wrapper.Mcpwarp, &body); err != nil {
			return nil, &DecodeError{Op: head.Op, Message: err.Error()}
		}
		services := make([]RegisteredService, 0, len(body.Services))
		for i, s := range body.Services {
			if s.Name == nil || *s.Name == "" {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("services[%d]: missing name", i)}
			}
			if s.ID == nil || *s.ID == "" {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("services[%d]: missing id", i)}
			}
			if s.Created == nil {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("services[%d]: missing created", i)}
			}
			url := ""
			if s.URL != nil {
				url = *s.URL
			}
			services = append(services, RegisteredService{Name: *s.Name, ID: *s.ID, URL: url, Created: *s.Created})
		}
		errs := make([]ServiceError, 0, len(body.Errors))
		for i, e := range body.Errors {
			if e.Name == nil || *e.Name == "" {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("errors[%d]: missing name", i)}
			}
			if e.Code == nil || *e.Code == "" {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("errors[%d]: missing code", i)}
			}
			if e.Message == nil {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("errors[%d]: missing message", i)}
			}
			errs = append(errs, ServiceError{Name: *e.Name, Code: *e.Code, Message: *e.Message})
		}
		return &Inbound{Op: OpRegistered, Services: services, Errors: errs}, nil

	case OpUnregistered:
		var body struct {
			Services []struct {
				Name *string `json:"name"`
				ID   *string `json:"id"`
			} `json:"services"`
		}
		if err := json.Unmarshal(wrapper.Mcpwarp, &body); err != nil {
			return nil, &DecodeError{Op: head.Op, Message: err.Error()}
		}
		services := make([]UnregisteredService, 0, len(body.Services))
		for i, s := range body.Services {
			if s.Name == nil || *s.Name == "" {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("services[%d]: missing name", i)}
			}
			if s.ID == nil || *s.ID == "" {
				return nil, &DecodeError{Op: head.Op, Message: fmt.Sprintf("services[%d]: missing id", i)}
			}
			services = append(services, UnregisteredService{Name: *s.Name, ID: *s.ID})
		}
		return &Inbound{Op: OpUnregistered, UnregisteredServices: services}, nil

	case OpDisable:
		var body struct {
			ID     *string `json:"id"`
			Reason *string `json:"reason"`
		}
		if err := json.Unmarshal(wrapper.Mcpwarp, &body); err != nil {
			return nil, &DecodeError{Op: head.Op, Message: err.Error()}
		}
		if body.ID == nil || *body.ID == "" {
			return nil, &DecodeError{Op: head.Op, Message: "missing id"}
		}
		if body.Reason == nil {
			return nil, &DecodeError{Op: head.Op, Message: "missing reason"}
		}
		return &Inbound{Op: OpDisable, DisableID: *body.ID, DisableReason: *body.Reason}, nil

	case OpEnable:
		var body struct {
			ID   *string `json:"id"`
			Name *string `json:"name"`
		}
		if err := json.Unmarshal(wrapper.Mcpwarp, &body); err != nil {
			return nil, &DecodeError{Op: head.Op, Message: err.Error()}
		}
		if body.ID == nil || *body.ID == "" {
			return nil, &DecodeError{Op: head.Op, Message: "missing id"}
		}
		if body.Name == nil || *body.Name == "" {
			return nil, &DecodeError{Op: head.Op, Message: "missing name"}
		}
		return &Inbound{Op: OpEnable, EnableID: *body.ID, EnableName: *body.Name}, nil

	case OpError:
		var body struct {
			Code    *string `json:"code"`
			Message *string `json:"message"`
		}
		if err := json.Unmarshal(wrapper.Mcpwarp, &body); err != nil {
			return nil, &DecodeError{Op: head.Op, Message: err.Error()}
		}
		if body.Code == nil || *body.Code == "" {
			return nil, &DecodeError{Op: head.Op, Message: "missing code"}
		}
		if body.Message == nil {
			return nil, &DecodeError{Op: head.Op, Message: "missing message"}
		}
		return &Inbound{Op: OpError, ErrorCode: *body.Code, ErrorMessage: *body.Message}, nil
	}

	return nil, nil // unreachable: knownOps gates the switch above
}
