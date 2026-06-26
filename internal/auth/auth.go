package auth

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/aync/syncbox/internal/db"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// jwtSecretKey 缓存 JWT 密钥，首次使用时从数据库读取或自动生成
var jwtSecretKey []byte

// getJWTSecret 获取 JWT 签名密钥，首次调用时自动生成并存入 settings 表
func getJWTSecret(database *db.DB) ([]byte, error) {
	if jwtSecretKey != nil {
		return jwtSecretKey, nil
	}

	// 尝试从数据库读取已有的 secret
	secret, err := database.GetSetting("jwt_secret")
	if err != nil {
		return nil, err
	}

	if secret == "" {
		// 首次使用，生成随机 secret
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return nil, err
		}
		secret = hex.EncodeToString(b)
		if err := database.SetSetting("jwt_secret", secret); err != nil {
			return nil, err
		}
	}

	jwtSecretKey = []byte(secret)
	return jwtSecretKey, nil
}

// SetJWTSecret 手动设置 JWT 密钥（用于测试或外部配置注入）
func SetJWTSecret(secret []byte) {
	jwtSecretKey = secret
}

// GenerateJWT 为管理员生成 JWT token，有效期 7 天
func GenerateJWT(database *db.DB, username string) (string, error) {
	secret, err := getJWTSecret(database)
	if err != nil {
		return "", err
	}

	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   username,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(7 * 24 * time.Hour)),
		Issuer:    "syncbox",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(secret)
}

// extractToken 从请求中提取 token
// 优先从 Authorization: Bearer xxx 头中获取，其次从 ?token=xxx 查询参数中获取
// 这样 SSE（EventSource）等无法设置请求头的场景也能正常认证
func extractToken(c *gin.Context) string {
	// 方式一：从请求头获取
	authHeader := c.GetHeader("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return parts[1]
		}
	}
	// 方式二：从查询参数获取（用于 SSE EventSource 等场景）
	if t := c.Query("token"); t != "" {
		return t
	}
	return ""
}

// AuthMiddleware 管理员 JWT 认证中间件
// 支持从 Authorization 头或 ?token= 查询参数中获取 JWT
func AuthMiddleware(database *db.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := extractToken(c)
		if tokenString == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "缺少认证信息"})
			c.Abort()
			return
		}

		// 获取签名密钥
		secret, err := getJWTSecret(database)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "获取认证密钥失败"})
			c.Abort()
			return
		}

		// 解析并验证 JWT
		token, err := jwt.ParseWithClaims(tokenString, &jwt.RegisteredClaims{}, func(t *jwt.Token) (interface{}, error) {
			// 确认签名方法为 HMAC
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return secret, nil
		})
		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "认证失败，token 无效或已过期"})
			c.Abort()
			return
		}

		// 验证通过，设置管理员标识
		c.Set("admin", true)
		c.Next()
	}
}

// DeviceAuthMiddleware 设备认证中间件
// 从 Authorization: Bearer xxx 中解析设备 token，验证设备存在且未被撤销
func DeviceAuthMiddleware(database *db.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头获取 Authorization
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "缺少认证信息"})
			c.Abort()
			return
		}

		// 解析 Bearer token
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "认证格式错误，应为 Bearer <token>"})
			c.Abort()
			return
		}
		tokenString := parts[1]

		// token 格式为 "设备ID:设备Token"
		tokenParts := strings.SplitN(tokenString, ":", 2)
		if len(tokenParts) != 2 {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "设备 token 格式错误，应为 <deviceID>:<token>"})
			c.Abort()
			return
		}

		deviceID := tokenParts[0]
		deviceToken := tokenParts[1]

		// 验证设备 token
		if !database.VerifyDeviceToken(deviceID, deviceToken) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "设备认证失败，token 无效或设备已撤销"})
			c.Abort()
			return
		}

		// 验证通过，设置设备标识
		c.Set("device_id", deviceID)
		c.Set("admin", false)
		c.Next()
	}
}
