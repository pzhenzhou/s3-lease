// Package common contains small process-level facilities shared by executable
// entry points and library components.
package common

import (
	"go.uber.org/zap"
)

var sharedLogger = zap.NewNop()

// BuildZapLogger builds the repository's standard structured logger.
// Production logging is JSON at Info level; development logging is a
// human-readable console at Debug level.
func BuildZapLogger(production bool) (*zap.Logger, error) {
	if production {
		return zap.NewProduction()
	}
	return zap.NewDevelopment()
}

// InitLogger builds the standard logger and falls back to a no-op logger if
// initialization fails. Library constructors should prefer BuildZapLogger when
// callers need to handle initialization errors explicitly.
func InitLogger(production bool) *zap.Logger {
	logger, err := BuildZapLogger(production)
	if err != nil {
		logger = zap.NewNop()
	}
	SetLogger(logger)
	return logger
}

// SetLogger installs the process-wide logger used by modules whose
// configuration does not provide an explicit logger. Call it during process
// startup, before starting goroutines that use Logger.
func SetLogger(logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}
	sharedLogger = logger
}

// Logger returns the process-wide logger. The returned Zap logger is safe for
// concurrent use and should be enriched per event rather than replaced per
// source file.
func Logger() *zap.Logger {
	return sharedLogger
}
