package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// CanonicalDocumentJSON is the normalized, unexpanded, redacted, sorted, compact identity form.
// The redactor is injected so pkg/config needs no dependency on the management layer.
func CanonicalDocumentJSON(data []byte, redact func(interface{}) interface{}) ([]byte, error) {
	if redact == nil {
		return nil, errors.New("canonical config hash requires a secret redactor")
	}
	raw, err := parseRawConfigMap(data)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		raw = map[string]interface{}{}
	}
	if normalizeErr := validateAndNormalizeRawConfig(raw); normalizeErr != nil {
		return nil, normalizeErr
	}
	value, err := canonicalHashValue(raw)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if encodeErr := encoder.Encode(redact(value)); encodeErr != nil {
		return nil, fmt.Errorf("encode canonical config: %w", encodeErr)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// CanonicalDocumentHash is the hex sha256 of CanonicalDocumentJSON.
func CanonicalDocumentHash(data []byte, redact func(interface{}) interface{}) (string, error) {
	canonical, err := CanonicalDocumentJSON(data, redact)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// canonicalHashValue converts yaml.v2 trees to JSON-encodable string-keyed maps.
// Non-string keys are stringified instead of dropped so they still affect the hash.
func canonicalHashValue(value interface{}) (interface{}, error) {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			converted, err := canonicalHashValue(nested)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			name := fmt.Sprint(key)
			if _, duplicate := out[name]; duplicate {
				return nil, fmt.Errorf("config key %q is ambiguous after stringification", name)
			}
			converted, err := canonicalHashValue(nested)
			if err != nil {
				return nil, err
			}
			out[name] = converted
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, nested := range typed {
			converted, err := canonicalHashValue(nested)
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	default:
		return typed, nil
	}
}
