package relay

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// isBodyless mirrors response-writer.ts's isBodyless: RFC 7230 §3.3
// forbids a body for 1xx, 204, 304, or any response to a HEAD request.
func isBodyless(status int, method string) bool {
	return status < 200 || status == 204 || status == 304 || method == http.MethodHead
}

// writeResponse serializes resp onto stream as raw HTTP/1.1 bytes: status
// line, headers minus hop-by-hop, then the body framed by resp's own
// Content-Length when known, else chunked — written and flushed
// incrementally (each upstream Read becomes one Write to stream, so an SSE
// body is delivered as it arrives rather than buffered in full) — ending
// with stream.CloseWrite(). Headers are written manually rather than via
// (*http.Response).Write so the body-copy loop is explicit and never
// buffers the whole response.
func writeResponse(stream Stream, resp *http.Response, method string) error {
	headers := stripHopByHop(resp.Header)
	headers.Del("Content-Length")

	bodyless := isBodyless(resp.StatusCode, method)
	knownLength := int64(-1)
	if !bodyless && resp.ContentLength >= 0 {
		knownLength = resp.ContentLength
		headers.Set("Content-Length", strconv.FormatInt(knownLength, 10))
	} else if !bodyless {
		headers.Set("Transfer-Encoding", "chunked")
	}

	if err := writeHead(stream, resp.StatusCode, headers); err != nil {
		return err
	}

	if !bodyless {
		if knownLength >= 0 {
			if _, err := io.CopyN(stream, resp.Body, knownLength); err != nil && err != io.EOF {
				return err
			}
		} else if err := copyChunked(stream, resp.Body); err != nil {
			return err
		}
	}

	return stream.CloseWrite()
}

func writeHead(w io.Writer, status int, headers http.Header) error {
	reason := http.StatusText(status)
	if reason == "" {
		reason = "Unknown Status"
	}
	if _, err := fmt.Fprintf(w, "HTTP/1.1 %d %s\r\n", status, reason); err != nil {
		return err
	}
	if err := headers.Write(w); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\r\n")
	return err
}

// copyChunked reads body in small pieces (so a slow, unbounded SSE stream is
// forwarded incrementally rather than buffered) and writes each as one HTTP
// chunk, ending with the final zero-length chunk.
func copyChunked(w io.Writer, body io.Reader) error {
	buf := make([]byte, 32*1024)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			if _, err := fmt.Fprintf(w, "%x\r\n", n); err != nil {
				return err
			}
			if _, err := w.Write(buf[:n]); err != nil {
				return err
			}
			if _, err := io.WriteString(w, "\r\n"); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	_, err := io.WriteString(w, "0\r\n\r\n")
	return err
}
