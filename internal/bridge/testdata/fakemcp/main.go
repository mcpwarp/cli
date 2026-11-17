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
		if mode, ok := strings.CutPrefix(arg, "--era="); ok {
			era = mode
		}
		if ms, ok := strings.CutPrefix(arg, "--discover-delay-ms="); ok {
			if n, err := strconv.Atoi(ms); err == nil {
				discoverDelay = time.Duration(n) * time.Millisecond
			}
		}
		if ms, ok := strings.CutPrefix(arg, "--initialize-delay-ms="); ok {
			if n, err := strconv.Atoi(ms); err == nil {
				initializeDelay = time.Duration(n) * time.Millisecond
			}
		}
		if n, ok := strings.CutPrefix(arg, "--discover-drop="); ok {
			if v, err := strconv.Atoi(n); err == nil {
				discoverDrop = v
			}
		}
		if ms, ok := strings.CutPrefix(arg, "--startup-delay-ms="); ok {
			if n, err := strconv.Atoi(ms); err == nil {
				startupDelay = time.Duration(n) * time.Millisecond
			}
		}
		if path, ok := strings.CutPrefix(arg, "--stdin-stall-until-file="); ok {
			stdinStallFile = path
		}
		if spec, ok := strings.CutPrefix(arg, "--discover-error="); ok {
			c, n, _ := strings.Cut(spec, ":")
			code, err1 := strconv.Atoi(c)
			count, err2 := strconv.Atoi(n)
			if err1 == nil && err2 == nil {
				discoverErrorCode, discoverErrorCount = code, count
			}
		}
		if c, ok := strings.CutPrefix(arg, "--reject-code="); ok {
			if n, err := strconv.Atoi(c); err == nil {
				rejectCode, rejectCodeSet = n, true
			}
		}
		switch arg {
		case "--discover-hold":
			discoverHold = true
		case "--initialize-hold":
			initializeHold = true
		case "--reject-modern-evidence":
			rejectModernEvidence = true
		case "--modern-bare-initialize-rejection":
			modernBareInitializeRejection = true
		case "--discover-no-versions":
			discoverNoVersions = true
		}
		if n, ok := strings.CutPrefix(arg, "--reject-initialize="); ok {
			if v, err := strconv.Atoi(n); err == nil {
				rejectInitialize = v
			}
		}
		if ms, ok := strings.CutPrefix(arg, "--crash-after-ms="); ok {
			if n, err := strconv.Atoi(ms); err == nil {
				go func(n int) {
					time.Sleep(time.Duration(n) * time.Millisecond)
					os.Exit(1)
				}(n)
			}
		}
	}

	// A stall reads nothing at all, so the OS pipe fills and the writer blocks.
	for stdinStallFile != "" {
		if _, err := os.Stat(stdinStallFile); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	lines := make(chan string, 4096)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 0, 64*1024), 32*1024*1024)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	// A slow start reads nothing for a while; then, like a server draining
	// its backlog, it learns of every cancellation in it before handling any
	// request in it.
	var backlog []string
	if startupDelay > 0 {
		time.Sleep(startupDelay)
	drain:
		for {
			select {
			case l, ok := <-lines:
				if !ok {
					break drain
				}
				backlog = append(backlog, l)
			default:
				break drain
			}
		}
		for _, l := range backlog {
			var m map[string]any
			if json.Unmarshal([]byte(l), &m) == nil && m["method"] == "notifications/cancelled" {
				p, _ := m["params"].(map[string]any)
				stateMu.Lock()
				cancelledRequestIDs[p["requestId"]] = true
				stateMu.Unlock()
			}
		}
	}
	for {
		var line string
		if len(backlog) > 0 {
			line, backlog = backlog[0], backlog[1:]
		} else {
			var ok bool
			if line, ok = <-lines; !ok {
				break
			}
		}
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

		if era != "" {
			handleEra(msg, method, id, hasID)
			continue
		}

		switch method {
		case "initialize":
			reply(id, map[string]any{"ok": true})
		case "server/discover":
			reply(id, map[string]any{
				"resultType":        "complete",
				"supportedVersions": []any{modernVersion, legacyVersion},
				"capabilities":      map[string]any{"tools": map[string]any{}},
				"ttlMs":             0,
				"cacheScope":        "private",
			})
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
