package grpc

import (
	"context"
	"encoding/json"
	"github.com/influxdata/flux"
)

// Request respresents the query to run.
type Request struct {
	// Command

	// Compiler converts the query to a specification to run against the data.
	Compiler flux.Compiler `json:"compiler"`

	// compilerMappings maps compiler types to creation methods
	compilerMappings flux.CompilerMappings
}

// WithCompilerMappings sets the query type mappings on the request.
func (r *Request) WithCompilerMappings(mappings flux.CompilerMappings) {
	r.compilerMappings = mappings
}

// UnmarshalJSON populates the request from the JSON data.
// WithCompilerMappings must have been called or an error will occur.
func (r *Request) UnmarshalJSON(data []byte) error {
	type Alias Request
	raw := struct {
		*Alias
		CompilerType flux.CompilerType `json:"compiler_type"`
		Compiler     json.RawMessage   `json:"compiler"`
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	createCompiler, ok := r.compilerMappings[raw.CompilerType]
	if !ok {
		//log.Printf("unsupported compiler type %q", raw.CompilerType)
	}

	c := createCompiler()
	if err := json.Unmarshal(raw.Compiler, c); err != nil {
		return err
	}
	r.Compiler = c

	return nil
}

func (r Request) MarshalJSON() ([]byte, error) {
	type Alias Request
	raw := struct {
		Alias
		CompilerType flux.CompilerType `json:"compiler_type"`
	}{
		Alias:        (Alias)(r),
		CompilerType: r.Compiler.CompilerType(),
	}
	return json.Marshal(raw)
}

type contextKey struct{}

var activeContextKey = contextKey{}

// ContextWithRequest returns a new context with a reference to the request.
func ContextWithRequest(ctx context.Context, req *Request) context.Context {
	return context.WithValue(ctx, activeContextKey, req)
}

//RequestFromContext retrieves a *Request from a context.
// If not request exists on the context nil is returned.
func RequestFromContext(ctx context.Context) *Request {
	v := ctx.Value(activeContextKey)
	if v == nil {
		return nil
	}
	return v.(*Request)
}
type dialect struct {

}

func (dialect) DialectType() flux.DialectType {
	return ""
}

func (dialect) Encoder() flux.MultiResultEncoder {
	return nil
}

// ProxyRequest specifies a query request and the dialect for the results.
type ProxyRequest struct {
	// Request is the basic query request
	Spec  flux.Spec `json:"request"`

	// Dialect is the result encoder
}






