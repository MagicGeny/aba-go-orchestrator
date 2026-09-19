package domain

import "strings"

// Structured worker / orchestrator error codes.
const (
	ErrorCodeUserNotFoundByPhone = "USER_NOT_FOUND_BY_PHONE"
	ErrorCodeInvalidPhone        = "INVALID_PHONE"
	ErrorCodeSessionExpired      = "SESSION_EXPIRED"
	ErrorCodeWorkerUnavailable   = "WORKER_UNAVAILABLE"
	ErrorCodeMaxUIError          = "MAX_UI_ERROR"
	ErrorCodeBrowserError        = "BROWSER_ERROR"
	ErrorCodeDeliveryUnknown     = "DELIVERY_UNKNOWN"
	ErrorCodeBlockedRecipient    = "BLOCKED_RECIPIENT"
	ErrorCodeAccountMismatch     = "ACCOUNT_MISMATCH"
)

// Account statuses for tenant_accounts.status
const (
	AccountStatusActive   = "active"
	AccountStatusCooldown = "cooldown"
	AccountStatusDisabled = "disabled"
)

// ErrorDisposition classifies how ResultConsumer / dosing should treat a failure.
type ErrorDisposition string

const (
	DispositionFinal     ErrorDisposition = "final"
	DispositionRetryable ErrorDisposition = "retryable"
	DispositionUnknown   ErrorDisposition = "unknown"
)

// ClassifyErrorCode maps a structured error_code to retry policy.
func ClassifyErrorCode(code string) ErrorDisposition {
	switch strings.ToUpper(strings.TrimSpace(code)) {
	case ErrorCodeUserNotFoundByPhone, ErrorCodeInvalidPhone, ErrorCodeBlockedRecipient, ErrorCodeAccountMismatch:
		return DispositionFinal
	case ErrorCodeDeliveryUnknown:
		return DispositionUnknown
	case ErrorCodeSessionExpired, ErrorCodeWorkerUnavailable, ErrorCodeMaxUIError, ErrorCodeBrowserError:
		return DispositionRetryable
	default:
		if code == "" {
			return DispositionFinal
		}
		// Unknown structured codes default to final to avoid duplicate sends.
		return DispositionFinal
	}
}

// IsSessionHealthError indicates the account should enter temporary cooldown.
func IsSessionHealthError(code string) bool {
	switch strings.ToUpper(strings.TrimSpace(code)) {
	case ErrorCodeSessionExpired, ErrorCodeBrowserError:
		return true
	default:
		return false
	}
}
