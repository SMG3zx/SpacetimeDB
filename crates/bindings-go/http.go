//go:build tinygo

package spacetimedb

import (
	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// ── HTTP Method ──────────────────────────────────────────────────────────────

// HttpMethod represents an HTTP request method as a BSATN sum type.
// Standard methods are represented as unit variants (tags 0–8).
// Extension methods use tag 9 with a string payload.
type HttpMethod uint8

const (
	HttpMethodGet     HttpMethod = 0
	HttpMethodHead    HttpMethod = 1
	HttpMethodPost    HttpMethod = 2
	HttpMethodPut     HttpMethod = 3
	HttpMethodDelete  HttpMethod = 4
	HttpMethodConnect HttpMethod = 5
	HttpMethodOptions HttpMethod = 6
	HttpMethodTrace   HttpMethod = 7
	HttpMethodPatch   HttpMethod = 8
)

// HttpMethodExtension represents a non-standard HTTP method.
const httpMethodExtensionTag uint8 = 9

// writeHttpMethod encodes an HttpMethod as BSATN.
func writeHttpMethod(w *bsatn.Writer, m HttpMethod) {
	w.WriteVariantTag(uint8(m))
	// Standard methods are unit variants — no payload.
}

// writeHttpMethodExtension encodes a custom extension HTTP method.
func writeHttpMethodExtension(w *bsatn.Writer, method string) {
	w.WriteVariantTag(httpMethodExtensionTag)
	w.WriteString(method)
}

// ── HTTP Version ─────────────────────────────────────────────────────────────

// HttpVersion represents an HTTP protocol version.
type HttpVersion uint8

const (
	HttpVersionHTTP09 HttpVersion = 0
	HttpVersionHTTP10 HttpVersion = 1
	HttpVersionHTTP11 HttpVersion = 2
	HttpVersionHTTP2  HttpVersion = 3
	HttpVersionHTTP3  HttpVersion = 4
)

// ── HTTP Header ──────────────────────────────────────────────────────────────

// HttpHeader represents a single HTTP header name-value pair.
type HttpHeader struct {
	Name  string
	Value []byte
}

// NewHttpHeader creates an HttpHeader from string name and value.
func NewHttpHeader(name, value string) HttpHeader {
	return HttpHeader{Name: name, Value: []byte(value)}
}

// ── HTTP Request ─────────────────────────────────────────────────────────────

// HttpRequest represents an outgoing HTTP request.
// Fields are ordered to match the Rust BSATN wire format (HttpRequestWire).
type HttpRequest struct {
	// Method is the HTTP method (GET, POST, etc.).
	Method HttpMethod
	// ExtensionMethod is used only when Method would be a non-standard value.
	// If non-empty, it overrides Method and is serialized as HttpMethod::Extension(string).
	ExtensionMethod string
	// Headers are the request headers.
	Headers []HttpHeader
	// TimeoutMicros is the optional request timeout in microseconds.
	// nil means no timeout. The host clamps to a maximum of 500ms.
	TimeoutMicros *int64
	// URI is the request URI.
	URI string
	// Version is the HTTP protocol version (default HTTP/1.1).
	Version HttpVersion
}

// WriteHttpRequest encodes an HttpRequest as BSATN matching the Rust wire format.
// Field order: Method, Headers, Timeout, Uri, Version.
func WriteHttpRequest(w *bsatn.Writer, req HttpRequest) {
	// Method: HttpMethod (sum type)
	if req.ExtensionMethod != "" {
		writeHttpMethodExtension(w, req.ExtensionMethod)
	} else {
		writeHttpMethod(w, req.Method)
	}
	// Headers: HttpHeaders { entries: Vec<HttpHeaderPair> }
	w.WriteArrayLen(uint32(len(req.Headers)))
	for _, h := range req.Headers {
		w.WriteString(h.Name)
		w.WriteArrayLen(uint32(len(h.Value)))
		w.WriteRaw(h.Value)
	}
	// Timeout: Option<HttpTimeoutWire>
	// HttpTimeoutWire is a product { timeout: TimeDuration { __time_duration_micros__: i64 } }
	if req.TimeoutMicros != nil {
		w.WriteVariantTag(0) // Some
		w.WriteI64(*req.TimeoutMicros)
	} else {
		w.WriteVariantTag(1) // None
	}
	// Uri: String
	w.WriteString(req.URI)
	// Version: HttpVersion (simple enum, serialized as variant tag)
	w.WriteVariantTag(uint8(req.Version))
}

// ── HTTP Response ────────────────────────────────────────────────────────────

// HttpResponse represents an incoming HTTP response.
// Fields are ordered to match the Rust BSATN wire format (HttpResponseWire).
type HttpResponse struct {
	// Headers are the response headers.
	Headers []HttpHeader
	// Version is the HTTP protocol version.
	Version HttpVersion
	// StatusCode is the HTTP status code (e.g., 200, 404).
	StatusCode uint16
}

// ReadHttpResponse decodes an HttpResponse from BSATN.
// Field order: Headers, Version, Code.
func ReadHttpResponse(r *bsatn.Reader) (HttpResponse, error) {
	var resp HttpResponse
	// Headers: HttpHeaders { entries: Vec<HttpHeaderPair> }
	numHeaders, err := r.ReadArrayLen()
	if err != nil {
		return resp, err
	}
	resp.Headers = make([]HttpHeader, numHeaders)
	for i := uint32(0); i < numHeaders; i++ {
		name, err := r.ReadString()
		if err != nil {
			return resp, err
		}
		resp.Headers[i].Name = name
		value, err := r.ReadBytes()
		if err != nil {
			return resp, err
		}
		resp.Headers[i].Value = value
	}
	// Version: HttpVersion (variant tag)
	versionTag, err := r.ReadVariantTag()
	if err != nil {
		return resp, err
	}
	resp.Version = HttpVersion(versionTag)
	// Code: u16
	code, err := r.ReadU16()
	if err != nil {
		return resp, err
	}
	resp.StatusCode = code
	return resp, nil
}
