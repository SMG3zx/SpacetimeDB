//go:build tinygo

package spacetimedb

import (
	"encoding/json"

	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// AuthCtx provides access to the authentication context for the current reducer call.
// JWT claims are loaded lazily on first access.
type AuthCtx struct {
	// IsInternal is true when the reducer was invoked internally (no client connection).
	// When true, GetJwt will always return nil.
	IsInternal bool

	connectionId *types.ConnectionId
	identity     types.Identity
	jwtLoaded    bool
	jwt          *JwtClaims
}

// newAuthCtxInternal creates an AuthCtx for an internal (no-connection) call.
func newAuthCtxInternal(identity types.Identity) AuthCtx {
	return AuthCtx{IsInternal: true, identity: identity}
}

// newAuthCtxFromConnection creates an AuthCtx for a client-connected call.
func newAuthCtxFromConnection(connectionId *types.ConnectionId, identity types.Identity) AuthCtx {
	return AuthCtx{IsInternal: connectionId == nil, connectionId: connectionId, identity: identity}
}

// GetJwt returns the JWT claims for this call, or nil if no JWT is available.
// The JWT is loaded from the host on first call and cached.
func (a *AuthCtx) GetJwt() *JwtClaims {
	if a.jwtLoaded {
		return a.jwt
	}
	a.jwtLoaded = true
	if a.IsInternal || a.connectionId == nil {
		return nil
	}
	src, err := sys.GetJwt(*a.connectionId)
	if err != nil || src == sys.InvalidBytesSource {
		return nil
	}
	payload, err := sys.ReadBytesSource(src)
	if err != nil || len(payload) == 0 {
		return nil
	}
	a.jwt = &JwtClaims{
		Identity:   a.identity,
		RawPayload: string(payload),
	}
	return a.jwt
}

// JwtClaims holds the JWT payload and provides lazy access to standard claims.
// The RawPayload is a UTF-8 JSON string containing the JWT claims.
type JwtClaims struct {
	// Identity is the identity of the caller.
	Identity types.Identity
	// RawPayload is the raw JWT claims JSON string.
	RawPayload string

	parsed   bool
	claims   jwtClaimsJSON
	parseErr error
}

// jwtClaimsJSON is used for lazy JSON deserialization of standard JWT fields.
type jwtClaimsJSON struct {
	Subject  string          `json:"sub"`
	Issuer   string          `json:"iss"`
	Audience json.RawMessage `json:"aud"`
}

// parse lazily deserializes the raw JWT JSON payload into the claims struct.
// Called automatically by Subject(), Issuer(), and Audience() on first access.
func (j *JwtClaims) parse() {
	if j.parsed {
		return
	}
	j.parsed = true
	j.parseErr = json.Unmarshal([]byte(j.RawPayload), &j.claims)
}

// Subject returns the "sub" claim from the JWT, or empty string if not present.
func (j *JwtClaims) Subject() string {
	j.parse()
	return j.claims.Subject
}

// Issuer returns the "iss" claim from the JWT, or empty string if not present.
func (j *JwtClaims) Issuer() string {
	j.parse()
	return j.claims.Issuer
}

// Audience returns the "aud" claim from the JWT.
// Per RFC 7519, "aud" can be a single string or an array of strings.
// Returns an empty slice if not present or on parse error.
func (j *JwtClaims) Audience() []string {
	j.parse()
	if j.parseErr != nil || len(j.claims.Audience) == 0 {
		return nil
	}
	// Try as a single string first
	var single string
	if err := json.Unmarshal(j.claims.Audience, &single); err == nil {
		return []string{single}
	}
	// Try as array of strings
	var arr []string
	if err := json.Unmarshal(j.claims.Audience, &arr); err == nil {
		return arr
	}
	return nil
}
