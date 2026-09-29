//go:build !windows && cgo

package apiserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/config"
	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/configledger"
	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/routerruntime"
)

func ledgerTestDocument(t *testing.T, decision string) []byte {
	t.Helper()
	doc, err := decodeYAMLDocument(mustMarshalCanonicalConfigYAML(t, minimalDeployTestConfig(decision)))
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := normalizeRouterConfigDocument(doc)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

func putLedgerTestDocument(t *testing.T, server *ClassificationAPIServer, document []byte) RouterConfigUpdateResponse {
	t.Helper()
	body, err := json.Marshal(RouterConfigUpdateRequest{YAML: string(document)})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, apiConfigPath, bytes.NewReader(body))
	setConfigPrecondition(t, request, server.configPath)
	recorder := httptest.NewRecorder()
	server.handleConfigPut(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response RouterConfigUpdateResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func rollbackLedgerTestVersion(t *testing.T, server *ClassificationAPIServer, version string) {
	t.Helper()
	body, err := json.Marshal(routerConfigRollbackRequest{Version: version})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, apiConfigRollbackPath, bytes.NewReader(body))
	setConfigPrecondition(t, request, server.configPath)
	recorder := httptest.NewRecorder()
	server.handleConfigRollback(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("rollback status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func documentHash(document []byte) string {
	digest := sha256.Sum256(document)
	return hex.EncodeToString(digest[:])
}

func TestConfigLedgerRecordsDeployAndRollbackGenerations(t *testing.T) {
	configPath := writeDeployTestBaseConfig(t)
	server := &ClassificationAPIServer{configPath: configPath}
	first := ledgerTestDocument(t, "first_route")
	second := ledgerTestDocument(t, "second_route")

	putLedgerTestDocument(t, server, first)
	backupOfFirst := putLedgerTestDocument(t, server, second).Version
	rollbackLedgerTestVersion(t, server, backupOfFirst)

	ledger := server.openConfigLedger(resolveConfigPersistencePaths(configPath))
	records := ledger.Records()
	if len(records) != 3 {
		t.Fatalf("ledger records = %+v, want 3 generations", records)
	}
	firstHash, err := config.CanonicalDocumentHash(first, redactSensitiveConfigValue)
	if err != nil {
		t.Fatal(err)
	}
	rollback := records[2]
	if rollback.Generation != 3 || rollback.Source != configledger.SourceRollback || rollback.Hash != firstHash ||
		rollback.RestoresGeneration != 1 || rollback.Version != backupOfFirst || rollback.State != configledger.StatePending {
		t.Fatalf("rollback record = %+v", rollback)
	}
	for _, record := range records[:2] {
		if record.State != configledger.StateSuperseded {
			t.Fatalf("older generation %d state = %s, want superseded", record.Generation, record.State)
		}
	}
	persisted, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ledger.Snapshot(3)
	if err != nil || !bytes.Equal(snapshot, persisted) {
		t.Fatalf("snapshot differs from the persisted document: err=%v", err)
	}

	recorder := httptest.NewRecorder()
	server.handleConfigVersions(recorder, httptest.NewRequest(http.MethodGet, apiConfigVersionsPath, nil))
	var versions []RouterConfigVersionEntry
	if err := json.Unmarshal(recorder.Body.Bytes(), &versions); err != nil {
		t.Fatal(err)
	}
	if len(versions) != 3 {
		t.Fatalf("ledger files leaked into /config/versions: %+v", versions)
	}
}

func TestConfigLedgerMarksObservedActivation(t *testing.T) {
	configPath := writeDeployTestBaseConfig(t)
	document := ledgerTestDocument(t, "active_route")
	registry := routerruntime.NewRegistry(&config.RouterConfig{DocumentHash: documentHash(document)})
	server := &ClassificationAPIServer{configPath: configPath, runtimeRegistry: registry}

	if response := putLedgerTestDocument(t, server, document); response.ActivationStatus != "active" {
		t.Fatalf("activation status = %s", response.ActivationStatus)
	}
	ledger := server.openConfigLedger(resolveConfigPersistencePaths(configPath))
	if active, ok := ledger.Active(); !ok || active.Generation != 1 || active.RuntimeHash != documentHash(document) {
		t.Fatalf("active record = %+v, %v", active, ok)
	}
}

func TestConfigLedgerReconcilesLateActivationAndRecoversAfterRestart(t *testing.T) {
	configPath := writeDeployTestBaseConfig(t)
	server := &ClassificationAPIServer{configPath: configPath, config: &config.RouterConfig{DocumentHash: "old"}}
	document := ledgerTestDocument(t, "late_route")
	if response := putLedgerTestDocument(t, server, document); response.ActivationStatus != "unknown" {
		t.Fatalf("activation status = %s", response.ActivationStatus)
	}
	paths := resolveConfigPersistencePaths(configPath)

	restarted := &ClassificationAPIServer{configPath: configPath, config: &config.RouterConfig{DocumentHash: "old"}}
	ledger := restarted.openConfigLedger(paths)
	if record, _ := ledger.Get(1); record.State != configledger.StatePending {
		t.Fatalf("pending generation matching the loaded document must resume, got %+v", record)
	}

	restarted.config = &config.RouterConfig{DocumentHash: documentHash(document)}
	ledger = restarted.openConfigLedger(paths)
	if record, _ := ledger.Get(1); record.State != configledger.StateActive {
		t.Fatalf("late activation was not reconciled: %+v", record)
	}
}

func TestConfigLedgerSkipsRejectedMutations(t *testing.T) {
	configPath := writeDeployTestBaseConfig(t)
	server := &ClassificationAPIServer{configPath: configPath}
	body, err := json.Marshal(RouterConfigUpdateRequest{YAML: string(ledgerTestDocument(t, "stale_route"))})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, apiConfigPath, bytes.NewReader(body))
	request.Header.Set("If-Match", `"stale"`)
	recorder := httptest.NewRecorder()
	server.handleConfigPut(recorder, request)
	if recorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d", recorder.Code)
	}
	journal := filepath.Join(filepath.Dir(configPath), ".vllm-sr", "config-backups", configledger.JournalFile)
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("a rejected mutation must not create a ledger journal")
	}
}

func TestConfigLedgerHashUsesManagementRedactor(t *testing.T) {
	document := func(secret string) []byte {
		return []byte("version: v0.3\nproviders:\n  models:\n    - name: m\n      backend_refs:\n" +
			"        - {endpoint: 127.0.0.1:8000, api_key: '" + secret + "', api_key_env: M_KEY}\n")
	}
	canonical, err := config.CanonicalDocumentJSON(document("sk-one"), redactSensitiveConfigValue)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(canonical, []byte("sk-one")) || !bytes.Contains(canonical, []byte(`"api_key":"`+redactedConfigValue+`"`)) ||
		!bytes.Contains(canonical, []byte(`"api_key_env":"M_KEY"`)) {
		t.Fatalf("canonical JSON = %s", canonical)
	}
	hash := func(secret string) string {
		value, hashErr := config.CanonicalDocumentHash(document(secret), redactSensitiveConfigValue)
		if hashErr != nil {
			t.Fatal(hashErr)
		}
		return value
	}
	if hash("sk-one") != hash("sk-two") {
		t.Fatal("inline secret values must not affect the canonical hash")
	}
	t.Setenv("M_SECRET", "expanded")
	withRef, err := config.CanonicalDocumentJSON(document("${M_SECRET}"), redactSensitiveConfigValue)
	if err != nil || !bytes.Contains(withRef, []byte(`"api_key":"${M_SECRET}"`)) {
		t.Fatalf("unexpanded env reference must stay visible: %s, %v", withRef, err)
	}
}
