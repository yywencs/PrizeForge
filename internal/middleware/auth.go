package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// ==================== Auth Types (ported from Kratos middleware, framework-independent) ====================

// AuthSource indicates how the user was authenticated.
type AuthSource string

const (
	AuthSourceGateway AuthSource = "gateway"
	AuthSourceJWT     AuthSource = "jwt"
	AuthSourceSession AuthSource = "session"
)

const (
	defaultAuthorizationHeader = "Authorization"
	defaultBearerPrefix        = "Bearer "
	defaultDeviceHeader        = "X-Device-Id"
	defaultSessionHeader       = "X-Session-Id"
	defaultGatewayUserHeader   = "X-User-Id"
	defaultGatewayIssuerHeader = "X-Auth-Issuer"
	defaultGatewayAudience     = "X-Auth-Audience"
	defaultGatewayExpireHeader = "X-Auth-Expires-At"
	defaultGatewaySignHeader   = "X-Auth-Signature"
	defaultGatewayTimeHeader   = "X-Auth-Timestamp"
	defaultGatewayNonceHeader  = "X-Auth-Nonce"
)

// AuthInfo holds the unified authentication result.
type AuthInfo struct {
	UserID    string
	SessionID string
	DeviceID  string
	Issuer    string
	Audience  []string
	Source    AuthSource
	Subject   string
	ExpiresAt time.Time
	TokenID   string
	RawToken  string
}

// AuthInfoKey is the Gin context key for storing AuthInfo.
const AuthInfoKey = "authInfo"

// SessionValidateRequest carries session validation data.
type SessionValidateRequest struct {
	SessionID string
	DeviceID  string
	Request   *http.Request
}

// GatewaySignatureRequest carries gateway HMAC verification data.
type GatewaySignatureRequest struct {
	Canonical string
	Signature string
	Header    http.Header
	AuthInfo  AuthInfo
}

// SessionValidator validates a session ID and returns auth info.
type SessionValidator func(c *gin.Context, req SessionValidateRequest) (*AuthInfo, error)

// GatewaySignatureValidator validates a gateway HMAC signature.
type GatewaySignatureValidator func(c *gin.Context, req GatewaySignatureRequest) error

// AuthOptions configures the authentication middleware.
type AuthOptions struct {
	Skipper func(c *gin.Context) bool
	Now     func() time.Time
	Sources []AuthSource
	JWT     JWTOptions
	Session SessionOptions
	Gateway GatewayOptions
}

// JWTOptions configures JWT bearer token authentication.
type JWTOptions struct {
	Enabled        bool
	Header         string
	Prefix         string
	CookieNames    []string
	KeyFunc        jwt.Keyfunc
	SigningKey     any
	SigningMethods []string
	Issuer         string
	Audience       []string
	DeviceHeader   string
	DeviceClaim    string
	UserIDClaims   []string
	SessionIDClaim string
	ClockSkew      time.Duration
	SubjectClaim   string
	TokenIDClaim   string
	ExpiryClaim    string
}

// SessionOptions configures session-based authentication.
type SessionOptions struct {
	Enabled  bool
	Header   string
	DeviceID bool
	// Validator is a pluggable session validator callback.
	Validator SessionValidator
}

// GatewayOptions configures gateway HMAC authentication.
type GatewayOptions struct {
	Enabled            bool
	Secret             string
	ClockSkew          time.Duration
	NonceTTL           time.Duration
	AllowedAlgorithms  []string
	UserIDHeader       string
	IssuerHeader       string
	AudienceHeader     string
	ExpiresAtHeader    string
	SignatureHeader    string
	TimestampHeader    string
	NonceHeader        string
	CanonicalHeaders   []string
	Validator          GatewaySignatureValidator
}

// ==================== Default Options ====================

// DefaultAuthOptions returns a sensible default configuration.
func DefaultAuthOptions() AuthOptions {
	return AuthOptions{
		Now: time.Now,
		Sources: []AuthSource{
			AuthSourceGateway,
			AuthSourceJWT,
			AuthSourceSession,
		},
		JWT: JWTOptions{
			Enabled:      true,
			Header:       defaultAuthorizationHeader,
			Prefix:       defaultBearerPrefix,
			DeviceHeader: defaultDeviceHeader,
			UserIDClaims: []string{"user_id", "uid", "sub"},
			ClockSkew:    30 * time.Second,
		},
		Session: SessionOptions{
			Enabled:  true,
			Header:   defaultSessionHeader,
			DeviceID: true,
		},
		Gateway: GatewayOptions{
			Enabled:           true,
			UserIDHeader:      defaultGatewayUserHeader,
			IssuerHeader:      defaultGatewayIssuerHeader,
			AudienceHeader:    defaultGatewayAudience,
			ExpiresAtHeader:   defaultGatewayExpireHeader,
			SignatureHeader:   defaultGatewaySignHeader,
			TimestampHeader:   defaultGatewayTimeHeader,
			NonceHeader:       defaultGatewayNonceHeader,
			AllowedAlgorithms: []string{"HMAC-SHA256"},
			ClockSkew:         5 * time.Minute,
		},
	}
}

// ==================== Gin Middleware ====================

// Auth creates a Gin middleware for authentication.
// It tries auth sources in order: Gateway HMAC → JWT Bearer → Session.
func Auth(opts AuthOptions) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check skipper
		if opts.Skipper != nil && opts.Skipper(c) {
			c.Next()
			return
		}

		var (
			info *AuthInfo
			err  error
		)

		for _, source := range opts.Sources {
			switch source {
			case AuthSourceGateway:
				if opts.Gateway.Enabled {
					info, err = authenticateGateway(c, opts)
					if err == nil {
						break
					}
				}
			case AuthSourceJWT:
				if opts.JWT.Enabled {
					info, err = authenticateJWT(c, opts)
					if err == nil {
						break
					}
				}
			case AuthSourceSession:
				if opts.Session.Enabled {
					info, err = authenticateSession(c, opts)
					if err == nil {
						break
					}
				}
			}
			if info != nil {
				break
			}
		}

		if info == nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				"code": 401,
				"info": "authentication required",
				"data": nil,
			})
			return
		}

		c.Set(AuthInfoKey, info)
		c.Next()
	}
}

// GetAuthInfo extracts AuthInfo from the Gin context.
func GetAuthInfo(c *gin.Context) *AuthInfo {
	if v, ok := c.Get(AuthInfoKey); ok {
		if info, ok := v.(*AuthInfo); ok {
			return info
		}
	}
	return nil
}

// ==================== Gateway HMAC Authentication ====================

func authenticateGateway(c *gin.Context, opts AuthOptions) (*AuthInfo, error) {
	gw := opts.Gateway
	now := opts.Now(); _ = now

	userID := c.GetHeader(gw.UserIDHeader)
	if userID == "" {
		return nil, fmt.Errorf("missing gateway user id header")
	}

	// Validate timestamp
	tsStr := c.GetHeader(gw.TimestampHeader)
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid gateway timestamp")
	}
	timestamp := time.Unix(ts, 0)
	if now.Sub(timestamp) > gw.ClockSkew || timestamp.Sub(now) > gw.ClockSkew {
		return nil, fmt.Errorf("gateway timestamp expired")
	}

	// Build canonical string for HMAC verification
	canonicalParts := []string{tsStr, c.GetHeader(gw.NonceHeader)}
	for _, h := range gw.CanonicalHeaders {
		canonicalParts = append(canonicalParts, c.GetHeader(h))
	}
	canonical := strings.Join(canonicalParts, "\n")

	info := AuthInfo{
		UserID: userID,
		Source: AuthSourceGateway,
	}

	// Validate signature
	sigHeader := c.GetHeader(gw.SignatureHeader)

	if gw.Validator != nil {
		req := GatewaySignatureRequest{
			Canonical: canonical,
			Signature: sigHeader,
			Header:    c.Request.Header,
			AuthInfo:  info,
		}
		if err := gw.Validator(c, req); err != nil {
			return nil, err
		}
	} else if gw.Secret != "" {
		// Verify HMAC-SHA256 signature
		expected := computeHMAC(gw.Secret, canonical)
		if !hmac.Equal([]byte(sigHeader), []byte(expected)) {
			// Also try hex encoding
			expectedHex := computeHMACHex(gw.Secret, canonical)
			if !hmac.Equal([]byte(sigHeader), []byte(expectedHex)) {
				return nil, fmt.Errorf("gateway signature mismatch")
			}
		}
	}

	// Extract optional fields
	if iss := c.GetHeader(gw.IssuerHeader); iss != "" {
		info.Issuer = iss
	}
	if aud := c.GetHeader(gw.AudienceHeader); aud != "" {
		info.Audience = []string{aud}
	}
	if expStr := c.GetHeader(gw.ExpiresAtHeader); expStr != "" {
		if exp, err := strconv.ParseInt(expStr, 10, 64); err == nil {
			info.ExpiresAt = time.Unix(exp, 0)
		}
	}

	return &info, nil
}

func computeHMAC(secret, message string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func computeHMACHex(secret, message string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

// ==================== JWT Authentication ====================

func authenticateJWT(c *gin.Context, opts AuthOptions) (*AuthInfo, error) {
	jwtOpts := opts.JWT
	now := opts.Now(); _ = now

	// Extract token from Authorization header or cookies
	tokenStr := extractJWTToken(c, jwtOpts)
	if tokenStr == "" {
		return nil, fmt.Errorf("no JWT token found")
	}

	// Determine key function
	keyFunc := jwtOpts.KeyFunc
	if keyFunc == nil && jwtOpts.SigningKey != nil {
		keyFunc = func(t *jwt.Token) (interface{}, error) {
			return jwtOpts.SigningKey, nil
		}
	}
	if keyFunc == nil {
		return nil, fmt.Errorf("no JWT key function configured")
	}

	// Parse and validate
	parser := jwt.NewParser(
		jwt.WithValidMethods(jwtOpts.SigningMethods),
		jwt.WithLeeway(jwtOpts.ClockSkew),
		jwt.WithIssuer(jwtOpts.Issuer),
		jwt.WithAudience(jwtOpts.Audience...),
	)

	token, err := parser.Parse(tokenStr, keyFunc)
	if err != nil {
		return nil, fmt.Errorf("jwt parse error: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid JWT claims")
	}

	// Extract user ID from configured claims
	userID := extractUserID(claims, jwtOpts.UserIDClaims)
	if userID == "" {
		return nil, fmt.Errorf("no user ID in JWT claims")
	}

	info := &AuthInfo{
		UserID:   userID,
		Source:   AuthSourceJWT,
		RawToken: tokenStr,
	}

	// Extract optional claims
	if sub, ok := claims[jwtOpts.SubjectClaim].(string); ok && jwtOpts.SubjectClaim != "" {
		info.Subject = sub
	}
	if sid, ok := claims[jwtOpts.SessionIDClaim].(string); ok && jwtOpts.SessionIDClaim != "" {
		info.SessionID = sid
	}
	if tid, ok := claims[jwtOpts.TokenIDClaim].(string); ok && jwtOpts.TokenIDClaim != "" {
		info.TokenID = tid
	}
	if exp, ok := claims[jwtOpts.ExpiryClaim].(float64); ok && jwtOpts.ExpiryClaim != "" {
		info.ExpiresAt = time.Unix(int64(exp), 0)
	}

	// Validate device binding
	if jwtOpts.DeviceClaim != "" {
		if deviceClaim, ok := claims[jwtOpts.DeviceClaim].(string); ok {
			deviceHeader := c.GetHeader(jwtOpts.DeviceHeader)
			if deviceHeader != "" && deviceClaim != deviceHeader {
				return nil, fmt.Errorf("device binding mismatch")
			}
			info.DeviceID = deviceClaim
		}
	}

	return info, nil
}

func extractJWTToken(c *gin.Context, opts JWTOptions) string {
	// Try Authorization header
	if opts.Header != "" {
		auth := c.GetHeader(opts.Header)
		if auth != "" && strings.HasPrefix(auth, opts.Prefix) {
			return strings.TrimPrefix(auth, opts.Prefix)
		}
	}
	// Try cookies
	for _, name := range opts.CookieNames {
		if cookie, err := c.Request.Cookie(name); err == nil {
			return cookie.Value
		}
	}
	return ""
}

func extractUserID(claims jwt.MapClaims, claimNames []string) string {
	for _, name := range claimNames {
		if v, ok := claims[name]; ok {
			switch val := v.(type) {
			case string:
				return val
			case float64:
				return strconv.FormatInt(int64(val), 10)
			}
		}
	}
	return ""
}

// ==================== Session Authentication ====================

func authenticateSession(c *gin.Context, opts AuthOptions) (*AuthInfo, error) {
	sessOpts := opts.Session

	sessionID := c.GetHeader(sessOpts.Header)
	if sessionID == "" {
		return nil, fmt.Errorf("no session id")
	}

	if sessOpts.Validator == nil {
		return nil, fmt.Errorf("no session validator configured")
	}

	var deviceID string
	if sessOpts.DeviceID {
		deviceID = c.GetHeader(defaultDeviceHeader)
	}

	req := SessionValidateRequest{
		SessionID: sessionID,
		DeviceID:  deviceID,
		Request:   c.Request,
	}

	info, err := sessOpts.Validator(c, req)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("session validation returned nil")
	}

	info.Source = AuthSourceSession
	info.SessionID = sessionID
	return info, nil
}

// ==================== Convenience: Noop Auth ====================

// NoopAuth returns a middleware that always passes (for testing).
func NoopAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(AuthInfoKey, &AuthInfo{
			UserID: "test-user",
			Source: AuthSourceJWT,
		})
		c.Next()
	}
}

// Ensure imports are used
