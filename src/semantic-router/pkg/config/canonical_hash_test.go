package config

import (
	"strings"
	"testing"
)

// testRedactSecrets stands in for the management redactor: api_key values
// become a placeholder unless they are a pure env reference.
func testRedactSecrets(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			if text, ok := nested.(string); key == "api_key" && (!ok || !strings.HasPrefix(text, "${")) {
				out[key] = "[REDACTED]"
				continue
			}
			out[key] = testRedactSecrets(nested)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, nested := range typed {
			out[i] = testRedactSecrets(nested)
		}
		return out
	default:
		return value
	}
}

const canonicalHashBaseYAML = `
version: v0.3
providers:
  defaults:
    model: model-a
  models:
    - name: model-a
      backend_refs:
        - endpoint: 127.0.0.1:8000
          provider: vllm
          weight: 1
          api_key: sk-inline-secret-one
          api_key_env: MODEL_A_KEY
routing:
  modelCards:
    - name: model-a
      description: "<default> & tier"
`

// Same document: reordered keys, comments, flow style, and 1.0 for 1.
const canonicalHashReformattedYAML = `
# operator comment
routing: {modelCards: [{description: "<default> & tier", name: model-a}]}
providers:
  models:
    - backend_refs:
        - {provider: vllm, api_key_env: MODEL_A_KEY, weight: 1.0, api_key: sk-inline-secret-one, endpoint: "127.0.0.1:8000"}
      name: model-a   # trailing comment
  defaults: {model: model-a}
version: v0.3
`

func mustCanonicalHash(t *testing.T, document string) string {
	t.Helper()
	hash, err := CanonicalDocumentHash([]byte(document), testRedactSecrets)
	if err != nil {
		t.Fatalf("CanonicalDocumentHash() error = %v", err)
	}
	return hash
}

func TestCanonicalDocumentHashIgnoresFormattingKeyOrderAndComments(t *testing.T) {
	first := mustCanonicalHash(t, canonicalHashBaseYAML)
	if second := mustCanonicalHash(t, canonicalHashReformattedYAML); first != second {
		t.Fatalf("reformatted document hash = %s, want %s", second, first)
	}
	if again := mustCanonicalHash(t, canonicalHashBaseYAML); again != first {
		t.Fatalf("hash is not stable across calls: %s then %s", first, again)
	}
	changed := strings.Replace(canonicalHashBaseYAML, "127.0.0.1:8000", "127.0.0.1:8001", 1)
	if mustCanonicalHash(t, changed) == first {
		t.Fatal("a semantic change must change the canonical hash")
	}
}

func TestCanonicalDocumentJSONIsSortedCompactAndUnescaped(t *testing.T) {
	canonical, err := CanonicalDocumentJSON([]byte(canonicalHashReformattedYAML), testRedactSecrets)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"providers":{"defaults":{"model":"model-a"},"models":[{"backend_refs":[{"api_key":"[REDACTED]","api_key_env":"MODEL_A_KEY","endpoint":"127.0.0.1:8000","provider":"vllm","weight":1}],"name":"model-a"}]},"routing":{"modelCards":[{"description":"<default> & tier","name":"model-a"}]},"version":"v0.3"}`
	if string(canonical) != want {
		t.Fatalf("canonical JSON =\n%s\nwant\n%s", canonical, want)
	}
}

func TestCanonicalDocumentHashReplacesSecretsWithPlaceholder(t *testing.T) {
	base := mustCanonicalHash(t, canonicalHashBaseYAML)
	rotated := strings.Replace(canonicalHashBaseYAML, "sk-inline-secret-one", "sk-inline-secret-two", 1)
	if got := mustCanonicalHash(t, rotated); got != base {
		t.Fatalf("inline secret value leaked into the hash: %s != %s", got, base)
	}
	canonical, err := CanonicalDocumentJSON([]byte(canonicalHashBaseYAML), testRedactSecrets)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), "sk-inline-secret") {
		t.Fatalf("canonical JSON leaked a secret: %s", canonical)
	}
	renamedEnv := strings.Replace(canonicalHashBaseYAML, "MODEL_A_KEY", "MODEL_B_KEY", 1)
	if mustCanonicalHash(t, renamedEnv) == base {
		t.Fatal("api_key_env names are not secrets and must affect the hash")
	}
}

func TestCanonicalDocumentHashUsesUnexpandedEnvironmentReferences(t *testing.T) {
	document := strings.Replace(canonicalHashBaseYAML, "sk-inline-secret-one", "${MODEL_A_SECRET}", 1)
	document = strings.Replace(document, "default> & tier", "${TIER_LABEL} tier", 1)

	t.Setenv("MODEL_A_SECRET", "first-value")
	t.Setenv("TIER_LABEL", "first")
	first := mustCanonicalHash(t, document)
	t.Setenv("MODEL_A_SECRET", "second-value")
	t.Setenv("TIER_LABEL", "second")
	if second := mustCanonicalHash(t, document); second != first {
		t.Fatalf("environment values changed the hash: %s != %s", second, first)
	}

	canonical, err := CanonicalDocumentJSON([]byte(document), testRedactSecrets)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"api_key":"${MODEL_A_SECRET}"`, `${TIER_LABEL} tier`} {
		if !strings.Contains(string(canonical), want) {
			t.Fatalf("canonical JSON %s is missing unexpanded reference %s", canonical, want)
		}
	}
	renamed := strings.Replace(document, "${MODEL_A_SECRET}", "${MODEL_A_OTHER}", 1)
	if mustCanonicalHash(t, renamed) == first {
		t.Fatal("renaming a secret env reference must change the hash")
	}
}

func TestCanonicalDocumentHashAppliesRawNormalization(t *testing.T) {
	legacyAlias := canonicalHashBaseYAML + `
global:
  stores:
    semantic_cache:
      enabled: true
`
	canonicalAlias := canonicalHashBaseYAML + `
global:
  stores:
    response_cache:
      enabled: true
`
	if mustCanonicalHash(t, legacyAlias) != mustCanonicalHash(t, canonicalAlias) {
		t.Fatal("the normalized alias and its canonical spelling must share one hash")
	}
}

func TestCanonicalDocumentHashRejectsInvalidDocuments(t *testing.T) {
	for name, document := range map[string]string{
		"yaml syntax":    "routing: [unterminated",
		"removed fields": canonicalHashBaseYAML + "evaluation_catalog: {}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := CanonicalDocumentHash([]byte(document), testRedactSecrets); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	if hash := mustCanonicalHash(t, ""); hash == "" {
		t.Fatal("an empty document still has a canonical hash")
	}
	if _, err := CanonicalDocumentHash([]byte(canonicalHashBaseYAML), nil); err == nil {
		t.Fatal("a nil redactor must be rejected rather than hashing plaintext secrets")
	}
}
