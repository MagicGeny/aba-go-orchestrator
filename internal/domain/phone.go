package domain

import "strings"

// NormalizePhone converts the phone number to canonical format (10 digits, starting with 9).
// Supports input in the format 7XXXXXXXXXX, 8XXXXXXXXXX, 9XXXXXXXXX, +7..., +8..., etc.
// If the phone number is not recognized, an empty string is returned.
//
// Used in:
// - usecase/campaign.go — when parsing Excel with a list of clients;
// - worker/result_consumer.go — when searching for the admin's chat_id in chat_phone_mappings.
func NormalizePhone(p string) string {
	var digits []rune
	for _, r := range p {
		if r >= '0' && r <= '9' {
			digits = append(digits, r)
		}
	}

	s := string(digits)
	if len(s) > 10 {
		if (s[0] == '7' || s[0] == '8') && s[1] == '9' {
			return s[1:]
		}
	}
	if len(s) == 10 && s[0] == '9' {
		return s
	}
	return ""
}

// ParseAdminPhones parses a string containing one or more admin phone numbers separated by semicolons.
// It splits the string by semicolon, trims spaces, filters out empty strings, and returns a slice of phone numbers.
// If the input is empty or contains no valid phone numbers after cleaning, returns an empty slice.
//
// Used in:
// - worker/result_consumer.go — when sending notifications to multiple admin phone numbers.
func ParseAdminPhones(phonesStr string) []string {
	if phonesStr == "" {
		return nil
	}

	var result []string
	parts := strings.Split(phonesStr, ";")
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
