package main

import (
	"strings"
	"time"
)

// Era modes (--era=<mode>) model how MCP servers of different protocol
// generations behave on stdio, per the 2026-07-28 versioning page's
// compatibility matrix. Without --era the fixture keeps its original
// era-agnostic behaviour (main.go).
//
//	legacy         2025-era: requests before initialize get a JSON-RPC error,
//	               server/discover is -32601.
//	legacy-silent  2025-era: nothing received before initialize is answered.
//	modern         2026-07-28 only: stateless, server/discover; initialize is
//	               rejected with -32022 naming the supported and requested
//	               versions.

const (
	metaVersionKey = "io.modelcontextprotocol/protocolVersion"
	metaCapsKey    = "io.modelcontextprotocol/clientCapabilities"

	modernVersion = "2026-07-28"
	legacyVersion = "2025-11-25"
)

var (
	era            string
	legacyInitDone bool

	// discoverDelay (--discover-delay-ms) delays a legacy child's -32601
	// answer to server/discover; discoverCount counts a modern child's
	// server/discover requests (guarded by stateMu).
	discoverDelay time.Duration
	discoverCount int
	// initializeDelay (--initialize-delay-ms) delays a legacy initialize
	// answer.
	initializeDelay time.Duration
	// rejectInitialize (--reject-initialize=N) makes the next N legacy
	// initialize requests fail with -32602, then behave normally.
	rejectInitialize int
	// discoverHold (--discover-hold) and initializeHold (--initialize-hold)
	// queue those answers, and debug/delayedError, debug/progressThenError
	// and debug/heldError (with "hold": true) queue theirs, until
	// debug/release sends them in order and then its own reply, so a test
	// knows exactly when they have been delivered. Guarded by stateMu.
	discoverHold   bool
	initializeHold bool
	held           []func()
	// discoverErrorCode (--discover-error=CODE) answers a modern
	// server/discover with error CODE. discoverNoVersions
	// (--discover-no-versions) leaves supportedVersions out of the
	// DiscoverResult.
	discoverErrorCode  int
	discoverNoVersions bool
	// requestCount counts the requests handled under an era mode, minus
	// server/discover and debug/* (guarded by stateMu).
	requestCount int
	// listenMode (--listen) makes a modern child serve subscriptions/listen:
	// it records the request id as the child saw it in openListens until a
	// notifications/cancelled names it, and sends the tagged acknowledgement.
	// Without it the method is unknown. Guarded by stateMu.
	// listenNoAck (--listen-no-ack) leaves the acknowledgement out.
	listenMode  bool
	listenNoAck bool
	openListens []any
	// cancelCounts counts the notifications/cancelled received per requestId
	// under an era mode (guarded by stateMu).
	cancelCounts = map[any]int{}
	// received lists, in the order they were read, the methods of every
	// message handled under an era mode, minus debug/* (guarded by stateMu).
	received []string
	// cancelMetas holds the _meta of the last such notification that had one.
	cancelMetas = map[any]any{}
)

func hold(f func()) {
	stateMu.Lock()
	held = append(held, f)
	stateMu.Unlock()
}

func replyError(id any, code int, message string, data any) {
	e := map[string]any{"code": code, "message": message}
	if data != nil {
		e["data"] = data
	}
	writeLine(map[string]any{"jsonrpc": "2.0", "id": id, "error": e})
}

// handleEra answers one parsed stdin message under the selected era mode.
func handleEra(msg map[string]any, method string, id any, hasID bool) {
	params, _ := msg["params"].(map[string]any)
	if method != "" && !strings.HasPrefix(method, "debug/") {
		stateMu.Lock()
		received = append(received, method)
		stateMu.Unlock()
	}
	if method == "notifications/cancelled" {
		stateMu.Lock()
		cancelledRequestIDs[params["requestId"]] = true
		cancelCounts[params["requestId"]]++
		if meta, ok := params["_meta"]; ok {
			cancelMetas[params["requestId"]] = meta
		}
		for i, open := range openListens {
			if open == params["requestId"] {
				openListens = append(openListens[:i:i], openListens[i+1:]...)
				break
			}
		}
		stateMu.Unlock()
		return
	}
	if !hasID || method == "" {
		return
	}
	meta, _ := params["_meta"].(map[string]any)
	_, modernShaped := meta[metaVersionKey]
	if strings.HasPrefix(method, "debug/") {
		handleEraDebug(method, id, params, meta)
		return
	}
	if method != "server/discover" {
		stateMu.Lock()
		requestCount++
		stateMu.Unlock()
	}

	switch era {
	case "legacy", "legacy-silent":
		handleLegacy(method, id, params)
	case "modern":
		handleModern(method, id, params, meta, modernShaped)
	}
}

func handleLegacy(method string, id any, params map[string]any) {
	if !legacyInitDone && method != "initialize" && era == "legacy-silent" {
		return
	}
	if method == "server/discover" {
		if discoverDelay > 0 {
			go func() {
				time.Sleep(discoverDelay)
				replyError(id, -32601, "Method not found: server/discover", nil)
			}()
			return
		}
		replyError(id, -32601, "Method not found: server/discover", nil)
		return
	}
	if method == "initialize" {
		meta, _ := params["_meta"].(map[string]any)
		if token, ok := meta["progressToken"]; ok {
			notify("notifications/progress", map[string]any{"progressToken": token, "progress": 1})
		}
		var answer func()
		if rejectInitialize > 0 {
			rejectInitialize--
			answer = func() { replyError(id, -32602, "Invalid params: bad initialize", nil) }
		} else {
			legacyInitDone = true
			result := map[string]any{
				"protocolVersion": legacyVersion,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "fakemcp-" + era, "version": "0"},
			}
			answer = func() { reply(id, result) }
		}
		switch {
		case initializeHold:
			hold(answer)
		case initializeDelay > 0:
			go func() {
				time.Sleep(initializeDelay)
				answer()
			}()
		default:
			answer()
		}
		return
	}
	if !legacyInitDone {
		replyError(id, -32600, "Server not initialized", nil)
		return
	}
	switch method {
	case "ping":
		reply(id, map[string]any{})
	case "tools/list":
		reply(id, map[string]any{"tools": []any{}})
	case "tools/call":
		reply(id, map[string]any{"content": []any{map[string]any{"type": "text", "text": "legacy:" + toolName(params)}}})
	default:
		replyError(id, -32601, "Method not found: "+method, nil)
	}
}

func handleModern(method string, id any, params, meta map[string]any, modernShaped bool) {
	if method == "initialize" {
		replyError(id, -32022, "Unsupported protocol version", map[string]any{
			"supported": []any{modernVersion},
			"requested": params["protocolVersion"],
		})
		return
	}
	if !modernShaped {
		replyError(id, -32602, "Invalid params: _meta."+metaVersionKey+" is required", nil)
		return
	}
	requested, _ := meta[metaVersionKey].(string)
	if requested != modernVersion {
		replyError(id, -32022, "Unsupported protocol version", map[string]any{
			"supported": []any{modernVersion},
			"requested": requested,
		})
		return
	}
	if _, ok := meta[metaCapsKey]; !ok {
		replyError(id, -32602, "Invalid params: _meta."+metaCapsKey+" is required", nil)
		return
	}
	switch method {
	case "server/discover":
		stateMu.Lock()
		discoverCount++
		stateMu.Unlock()
		if discoverErrorCode != 0 {
			replyError(id, discoverErrorCode, "discover failed", nil)
			return
		}
		result := discoverResult()
		if discoverNoVersions {
			delete(result, "supportedVersions")
		}
		if discoverHold {
			hold(func() { reply(id, result) })
			return
		}
		reply(id, result)
	case "tools/list":
		reply(id, map[string]any{"resultType": "complete", "tools": []any{}, "ttlMs": 0, "cacheScope": "private"})
	case "tools/call":
		reply(id, map[string]any{
			"resultType": "complete",
			"content":    []any{map[string]any{"type": "text", "text": "modern:" + toolName(params)}},
		})
	case "subscriptions/listen":
		stateMu.Lock()
		supported, ack := listenMode, !listenNoAck
		if supported {
			openListens = append(openListens, id)
		}
		stateMu.Unlock()
		if !supported {
			replyError(id, -32601, "Method not found: "+method, nil)
			return
		}
		if !ack {
			return
		}
		var notifications any = map[string]any{}
		if n, ok := params["notifications"]; ok {
			notifications = n
		}
		notifyTagged("notifications/subscriptions/acknowledged", map[string]any{"notifications": notifications}, id)
	default:
		replyError(id, -32601, "Method not found: "+method, nil)
	}
}

func discoverResult() map[string]any {
	return map[string]any{
		"resultType":        "complete",
		"supportedVersions": []any{modernVersion},
		"capabilities":      map[string]any{"tools": map[string]any{}},
		"ttlMs":             0,
		"cacheScope":        "private",
	}
}

// handleEraDebug serves the fixture's debug/* methods under any --era mode,
// ignoring initialize state and request shape, so tests can poke a child
// the bridge considers legacy or uninitialised.
//
//	debug/wasCancelled     {requestId}               -> {cancelled}
//	debug/discoverCount                              -> {count}
//	debug/requestCount                               -> {count}
//	debug/heldError         {code, message?}         an error answer held until
//	                                                 debug/release
//	debug/release                                    sends every held answer in
//	                                                 order, then -> {released}
//	debug/cancelCount      {requestId}               -> {count} notifications/cancelled
//	                                                 received for it
//	debug/received                                   -> {methods} every method read so
//	                                                 far, in order, minus debug/*
//	debug/cancelMeta       {requestId}               -> {hasMeta, meta} of the last
//	                                                 cancel for it that carried _meta
//	debug/listens                                    -> {ids} the subscriptions/listen
//	                                                 ids the child has seen and not
//	                                                 had cancelled (--listen)
//	debug/raw              {lines}                   writes each string to stdout
//	                                                 verbatim, in one write, then
//	                                                 -> {written}
//	debug/flood            {subscriptionId, count, size}
//	                                                 count notifications tagged with
//	                                                 subscriptionId, each carrying
//	                                                 size bytes, then -> {sent}
//	debug/delayedError     {delayMs, code, message?} error after the delay
//	debug/progressThenError {delayMs, code}          a progress notification for
//	                                                 _meta.progressToken now, then
//	                                                 the error after the delay
func handleEraDebug(method string, id any, params, meta map[string]any) {
	delay := time.Duration(0)
	if d, ok := params["delayMs"].(float64); ok {
		delay = time.Duration(d) * time.Millisecond
	}
	code := -32601
	if c, ok := params["code"].(float64); ok {
		code = int(c)
	}
	message, _ := params["message"].(string)
	if message == "" {
		message = "debug error"
	}
	switch method {
	case "debug/wasCancelled":
		stateMu.Lock()
		cancelled := cancelledRequestIDs[params["requestId"]]
		stateMu.Unlock()
		reply(id, map[string]any{"cancelled": cancelled})
	case "debug/cancelCount":
		stateMu.Lock()
		n := cancelCounts[params["requestId"]]
		stateMu.Unlock()
		reply(id, map[string]any{"count": n})
	case "debug/received":
		stateMu.Lock()
		methods := make([]any, len(received))
		for i, m := range received {
			methods[i] = m
		}
		stateMu.Unlock()
		reply(id, map[string]any{"methods": methods})
	case "debug/cancelMeta":
		stateMu.Lock()
		meta, has := cancelMetas[params["requestId"]]
		stateMu.Unlock()
		reply(id, map[string]any{"hasMeta": has, "meta": meta})
	case "debug/listens":
		stateMu.Lock()
		ids := append([]any{}, openListens...)
		stateMu.Unlock()
		reply(id, map[string]any{"ids": ids})
	case "debug/raw":
		var lines []string
		raw, _ := params["lines"].([]any)
		for _, l := range raw {
			if s, ok := l.(string); ok {
				lines = append(lines, s)
			}
		}
		writeRawLines(lines)
		reply(id, map[string]any{"written": len(lines)})
	case "debug/flood":
		count, _ := params["count"].(float64)
		size, _ := params["size"].(float64)
		payload := strings.Repeat("x", int(size))
		for i := 0; i < int(count); i++ {
			notifyTagged("notifications/flood", map[string]any{"i": i, "payload": payload}, params["subscriptionId"])
		}
		reply(id, map[string]any{"sent": int(count)})
	case "debug/release":
		stateMu.Lock()
		queued := held
		held = nil
		stateMu.Unlock()
		for _, f := range queued {
			f()
		}
		reply(id, map[string]any{"released": len(queued)})
	case "debug/requestCount":
		stateMu.Lock()
		n := requestCount
		stateMu.Unlock()
		reply(id, map[string]any{"count": n})
	case "debug/discoverCount":
		stateMu.Lock()
		n := discoverCount
		stateMu.Unlock()
		reply(id, map[string]any{"count": n})
	case "debug/delayedError", "debug/progressThenError", "debug/heldError":
		if method == "debug/progressThenError" {
			notify("notifications/progress", map[string]any{"progressToken": meta["progressToken"], "progress": 1})
		}
		if h, _ := params["hold"].(bool); h || method == "debug/heldError" {
			hold(func() { replyError(id, code, message, nil) })
			return
		}
		go func() {
			time.Sleep(delay)
			replyError(id, code, message, nil)
		}()
	default:
		replyError(id, -32601, "Method not found: "+method, nil)
	}
}

func toolName(params map[string]any) string {
	n, _ := params["name"].(string)
	return n
}
