package ui

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// WithBasicAuth enables HTTP Basic Authentication for the UI dashboard and REST APIs.
func WithBasicAuth(username, password string) Option {
	return func(o *Options) {
		mw := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u, p, ok := r.BasicAuth()
				if !ok || u != username || p != password {
					w.Header().Set("WWW-Authenticate", `Basic realm="OrderedJob Dashboard"`)
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		}
		o.AuthMiddleware = ChainMiddlewares(o.AuthMiddleware, mw)
	}
}

// WithTokenAuth protects the UI dashboard using a secret bearer token.
// The token can be passed via 'Authorization: Bearer <token>' header or '?token=<token>' query parameter.
func WithTokenAuth(expectedToken string) Option {
	return func(o *Options) {
		mw := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				token := extractToken(r)
				if token == "" || token != expectedToken {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		}
		o.AuthMiddleware = ChainMiddlewares(o.AuthMiddleware, mw)
	}
}

// WithJWTAuth protects the UI dashboard using HMAC-SHA256 (HS256) JWT signature verification.
// The JWT token can be passed via 'Authorization: Bearer <token>' header or '?token=<token>' query parameter.
func WithJWTAuth(secretKey string) Option {
	return func(o *Options) {
		mw := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tokenStr := extractToken(r)
				if tokenStr == "" || !verifyHS256JWT(tokenStr, secretKey) {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		}
		o.AuthMiddleware = ChainMiddlewares(o.AuthMiddleware, mw)
	}
}

// WithJWTValidator protects the UI dashboard using a custom JWT/token validator function.
func WithJWTValidator(validator func(tokenString string) bool) Option {
	return func(o *Options) {
		mw := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tokenStr := extractToken(r)
				if tokenStr == "" || validator == nil || !validator(tokenStr) {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		}
		o.AuthMiddleware = ChainMiddlewares(o.AuthMiddleware, mw)
	}
}

// WithAuthFunc protects the UI dashboard using a custom authentication evaluator function on *http.Request.
func WithAuthFunc(authFunc func(r *http.Request) bool) Option {
	return func(o *Options) {
		mw := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if authFunc == nil || !authFunc(r) {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		}
		o.AuthMiddleware = ChainMiddlewares(o.AuthMiddleware, mw)
	}
}

// WithAuthMiddleware wraps the UI handler with custom HTTP middleware.
func WithAuthMiddleware(mw func(http.Handler) http.Handler) Option {
	return func(o *Options) {
		o.AuthMiddleware = ChainMiddlewares(o.AuthMiddleware, mw)
	}
}

// ChainMiddlewares combines two HTTP middlewares into a single middleware.
func ChainMiddlewares(m1, m2 func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	if m1 == nil {
		return m2
	}
	if m2 == nil {
		return m1
	}
	return func(next http.Handler) http.Handler {
		return m1(m2(next))
	}
}

// extractToken retrieves a bearer token from Authorization header or ?token query param.
func extractToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return strings.TrimSpace(authHeader[7:])
	}
	return r.URL.Query().Get("token")
}

// verifyHS256JWT validates an HS256 signed JWT string and checks expiration ('exp').
func verifyHS256JWT(tokenStr, secret string) bool {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return false
	}

	headerB64, payloadB64, signatureB64 := parts[0], parts[1], parts[2]

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(headerB64 + "." + payloadB64))
	expectedSig := mac.Sum(nil)

	actualSig, err := decodeBase64URL(signatureB64)
	if err != nil {
		return false
	}

	if !hmac.Equal(expectedSig, actualSig) {
		return false
	}

	payloadBytes, err := decodeBase64URL(payloadB64)
	if err != nil {
		return false
	}

	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err == nil && claims.Exp > 0 {
		if time.Now().Unix() > claims.Exp {
			return false
		}
	}

	return true
}

func decodeBase64URL(s string) ([]byte, error) {
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.URLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.StdEncoding.DecodeString(s)
}

// GenerateHS256JWT creates a signed HS256 JWT token string with optional claims and expiration duration.
func GenerateHS256JWT(secret string, claims map[string]any, expiryDuration time.Duration) (string, error) {
	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}
	headerBytes, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	if claims == nil {
		claims = make(map[string]any)
	}
	if expiryDuration != 0 {
		claims["exp"] = time.Now().Add(expiryDuration).Unix()
	}

	payloadBytes, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	unsignedToken := headerB64 + "." + payloadB64

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(unsignedToken))
	sigBytes := mac.Sum(nil)
	sigB64 := base64.RawURLEncoding.EncodeToString(sigBytes)

	return unsignedToken + "." + sigB64, nil
}
