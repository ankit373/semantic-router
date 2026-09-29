//go:build !windows && cgo

package apiserver

import (
	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/config"
	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/configledger"
	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/k8s/configwriter"
	"github.com/vllm-project/semantic-router/src/semantic-router/pkg/observability/logging"
)

const configLedgerReasonActivationFailed = "ActivationFailed"

// configLedgerCommit tracks one recorded generation until its activation is observed.
// The ledger records mutations but never changes their HTTP outcome.
type configLedgerCommit struct {
	ledger     *configledger.Ledger
	generation int64
}

// openConfigLedger returns the file-target ledger, recovering it once per
// directory. It must run before the mutation writes the source document.
func (s *ClassificationAPIServer) openConfigLedger(paths configPersistencePaths) *configledger.Ledger {
	if _, ok := configwriter.ConfigMapTargetFromEnv(); ok {
		return nil
	}
	dir := configBackupDir(paths.sourcePath)
	s.configLedgerMu.Lock()
	defer s.configLedgerMu.Unlock()
	if s.configLedger == nil || s.configLedger.Dir() != dir {
		loadedHash := ""
		if data, err := readPersistedSourceConfig(paths.sourcePath); err == nil && len(data) > 0 {
			loadedHash, _ = config.CanonicalDocumentHash(data, redactSensitiveConfigValue)
		}
		ledger, err := configledger.Open(dir, configledger.Options{Retain: maxBackups, LoadedHash: loadedHash})
		if err != nil {
			logging.Warnf("Config ledger unavailable in %s: %v", dir, err)
			return nil
		}
		s.configLedger = ledger
	}
	s.reconcileConfigLedger(s.configLedger)
	return s.configLedger
}

// reconcileConfigLedger settles pending generations whose activation finished
// after the mutation response returned.
func (s *ClassificationAPIServer) reconcileConfigLedger(ledger *configledger.Ledger) {
	activeHash := s.activeConfigDocumentHash()
	for _, record := range ledger.Records() {
		if record.State != configledger.StatePending || record.RuntimeHash == "" {
			continue
		}
		commit := configLedgerCommit{ledger: ledger, generation: record.Generation}
		if record.RuntimeHash == activeHash {
			commit.observe("active")
		} else if activation := s.configActivation(record.RuntimeHash); activation != nil && activation.Status == "failed" {
			commit.observe("failed")
		}
	}
}

// recordConfigLedgerPending journals a persisted document as a new pending generation.
func (s *ClassificationAPIServer) recordConfigLedgerPending(
	ledger *configledger.Ledger,
	paths configPersistencePaths,
	document []byte,
	version string,
	source string,
) configLedgerCommit {
	if ledger == nil {
		return configLedgerCommit{}
	}
	hash, err := config.CanonicalDocumentHash(document, redactSensitiveConfigValue)
	if err != nil {
		logging.Warnf("Config ledger skipped version %s: canonical hash failed: %s", version, scrubSecretsInErrorMessage(err.Error()))
		return configLedgerCommit{}
	}
	runtimeHash, _ := configFileHash(paths.runtimePath)
	record, err := ledger.Append(configledger.Entry{
		Hash: hash, RuntimeHash: runtimeHash, Version: version, Source: source, Document: document,
	})
	if err != nil {
		logging.Warnf("Config ledger failed to record version %s: %v", version, err)
		if record.Generation == 0 {
			return configLedgerCommit{}
		}
	}
	return configLedgerCommit{ledger: ledger, generation: record.Generation}
}

// observe applies the activation status reported to the client, if it is terminal.
func (c configLedgerCommit) observe(runtimeStatus string) {
	if c.ledger == nil || c.generation == 0 {
		return
	}
	var err error
	switch runtimeStatus {
	case "active":
		_, err = c.ledger.MarkActive(c.generation)
	case "failed":
		_, err = c.ledger.MarkFailed(c.generation, configLedgerReasonActivationFailed)
	default:
		return
	}
	if err != nil {
		logging.Warnf("Config ledger failed to mark generation %d %s: %v", c.generation, runtimeStatus, err)
	}
}
