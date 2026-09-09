// Command fakemcp is a minimal NDJSON stdio MCP server used only by
// internal/bridge's e2e tests (built via `go build` into t.TempDir by the
// test itself). Ported from mcpwarp-cli's
// test/fixtures/fake-mcp-stdio.mjs — see that file's header for the full
// method-by-method behaviour this mirrors (initialize/ping/tools/list,
// notifications/emit, server/request(-1), slow, with-progress,
// subscriptions/listen+push+finish, notifications/cancelled +
// debug/wasCancelled, hang, debug/crash, --crash-after-ms=<n>) — plus one
// non-JSON banner line on stdout and one on stderr at startup, which the
// bridge/stdio child must tolerate, and a Go-only debug/replyThenExit
// case (blocker 1's os/exec Wait/pipe-race regression test).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const subscriptionIDMetaKey = "io.modelcontextprotocol/subscriptionId"

// stdoutMu serializes writes: main's reply/notify calls run alongside the
// "slow"/server-request-response goroutines and callbacks.
var stdoutMu sync.Mutex

func writeLine(v map[string]any) {
	b, _ := json.Marshal(v)
	stdoutMu.Lock()
	fmt.Println(string(b))
	stdoutMu.Unlock()
}

func reply(id any, result any) {
	writeLine(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func notify(method string, params any) {
	writeLine(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func notifyTagged(method string, params map[string]any, subscriptionID any) {
	tagged := make(map[string]any, len(params)+1)
	for k, v := range params {
		tagged[k] = v
	}
	tagged["_meta"] = map[string]any{subscriptionIDMetaKey: subscriptionID}
	notify(method, tagged)
}

func sendServerRequest(id any, method string, params any) {
	writeLine(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

// state guards the fixture's small bit of cross-line memory: the one
// in-flight server-initiated request this fixture ever has open at a
// time, and the set of requestIds it's seen cancelled.
var (
	stateMu             sync.Mutex
	pendingServerReqID  any
	pendingServerReqSet bool
	onServerResponse    func(map[string]any)
	cancelledRequestIDs = map[any]bool{}
)

func main() {
	fmt.Println("fake-mcp-stdio starting up (not JSON)")
	fmt.Fprintln(os.Stderr, "fake-mcp-stdio: ready on stderr")

	for _, arg := range os.Args[1:] {
		if ms, ok := strings.CutPrefix(arg, "--crash-after-ms="); ok {
			if n, err := strconv.Atoi(ms); err == nil {
				go func(n int) {
					time.Sleep(time.Duration(n) * time.Millisecond)
					os.Exit(1)
				}(n)
			}
		}
	}

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 32*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}
		methodVal, hasMethod := msg["method"]
		method, _ := methodVal.(string)
		id, hasID := msg["id"]

		stateMu.Lock()
		if pendingServerReqSet && !hasMethod && id == pendingServerReqID {
			cb := onServerResponse
			pendingServerReqSet = false
			pendingServerReqID = nil
			onServerResponse = nil
			stateMu.Unlock()
			if cb != nil {
				cb(msg)
			}
			continue
		}
		stateMu.Unlock()

		switch method {
		case "initialize":
			reply(id, map[string]any{"ok": true})
		case "ping":
			reply(id, "pong")
		case "tools/list":
			reply(id, map[string]any{"tools": []any{}})
		case "notifications/emit":
			reply(id, map[string]any{"ok": true})
			notify("notify", map[string]any{})
		case "server/request":
			originalID := id
			stateMu.Lock()
			pendingServerReqID, pendingServerReqSet = "srv-1", true
			onServerResponse = func(response map[string]any) {
				notify("server/ack", map[string]any{"ackedResult": response["result"]})
				reply(originalID, map[string]any{"ok": true})
			}
			stateMu.Unlock()
			sendServerRequest("srv-1", "server/ping", map[string]any{})
		case "server/request-1":
			// Deliberately collides its own id (the number 1) with
			// whatever internal id the bridge may have minted 1 for on a
			// concurrently in-flight client request — proves shape-based
			// classification rather than id-based routing.
			originalID := id
			stateMu.Lock()
			pendingServerReqID, pendingServerReqSet = float64(1), true
			onServerResponse = func(response map[string]any) {
				notify("server/ack", map[string]any{"ackedResult": response["result"]})
				reply(originalID, map[string]any{"ok": true})
			}
			stateMu.Unlock()
			sendServerRequest(1, "server/ping", map[string]any{})
		case "slow":
			delayMs := 200.0
			if params, ok := msg["params"].(map[string]any); ok {
				if d, ok := params["delayMs"].(float64); ok {
					delayMs = d
				}
			}
			go func(id any, delayMs float64) {
				time.Sleep(time.Duration(delayMs) * time.Millisecond)
				reply(id, map[string]any{"ok": true})
			}(id, delayMs)
		case "with-progress":
			var token any
			if params, ok := msg["params"].(map[string]any); ok {
				if meta, ok := params["_meta"].(map[string]any); ok {
					token = meta["progressToken"]
				}
			}
			notify("notifications/progress", map[string]any{"progressToken": token, "progress": 1, "total": 1})
			reply(id, map[string]any{"ok": true})
		case "subscriptions/listen":
			var notifications any = map[string]any{}
			if params, ok := msg["params"].(map[string]any); ok {
				if n, ok := params["notifications"]; ok {
					notifications = n
				}
			}
			notifyTagged("notifications/subscriptions/acknowledged", map[string]any{"notifications": notifications}, id)
		case "subscriptions/push":
			params, _ := msg["params"].(map[string]any)
			subscriptionID := params["subscriptionId"]
			notifyMethod, _ := params["notifyMethod"].(string)
			notifyParams, _ := params["notifyParams"].(map[string]any)
			if notifyParams == nil {
				notifyParams = map[string]any{}
			}
			notifyTagged(notifyMethod, notifyParams, subscriptionID)
		case "subscriptions/finish":
			params, _ := msg["params"].(map[string]any)
			subscriptionID := params["subscriptionId"]
			result := params["result"]
			if result == nil {
				result = map[string]any{"resultType": "complete"}
			}
			reply(subscriptionID, result)
		case "notifications/cancelled":
			if params, ok := msg["params"].(map[string]any); ok {
				stateMu.Lock()
				cancelledRequestIDs[params["requestId"]] = true
				stateMu.Unlock()
			}
		case "debug/wasCancelled":
			var requestID any
			if params, ok := msg["params"].(map[string]any); ok {
				requestID = params["requestId"]
			}
			stateMu.Lock()
			cancelled := cancelledRequestIDs[requestID]
			stateMu.Unlock()
			reply(id, map[string]any{"cancelled": cancelled})
		case "hang":
			// intentionally never replies
		case "debug/replyThenExit":
			reply(id, map[string]any{"ok": true})
			os.Exit(0)
		case "debug/crash":
			code := 1
			if params, ok := msg["params"].(map[string]any); ok {
				if c, ok := params["code"].(float64); ok {
					code = int(c)
				}
			}
			os.Exit(code)
		default:
			if hasID {
				reply(id, map[string]any{"error": "unknown method"})
			}
		}
	}
}
