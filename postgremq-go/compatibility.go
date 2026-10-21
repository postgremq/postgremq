package postgremq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// supportedMQVersions are the mq (schema) compatibility lines this client
// implements and is tested against. A breaking schema change starts a new
// line: it bumps the mq major, or before 1.0 the minor.
var supportedMQVersions = []string{"0.1"}

// SupportedMQVersions returns the mq (schema) compatibility lines this client
// implements: an mq major ("1"), or before 1.0 a major and minor ("0.1").
// Dial and DialFromPool reject a database whose schema version, as
// postgremq.info() reports it, is on any other line.
func SupportedMQVersions() []string { return slices.Clone(supportedMQVersions) }

// mqLine is the compatibility line of a stored schema version: its mq major,
// or before 1.0 its major and minor.
func mqLine(version int64) string {
	if major := version / 100_000_000; major > 0 {
		return strconv.FormatInt(major, 10)
	}
	return "0." + strconv.FormatInt(version/10_000%10_000, 10)
}

// ErrIncompatibleSchema matches every *CompatibilityError.
var ErrIncompatibleSchema = errors.New("postgremq: incompatible database installation")

// CompatibilityError is returned by Dial and DialFromPool when the database's
// PostgreMQ schema is on an mq line this client does not support, reports no
// schema version, or has no discovery function (postgremq.info()) and needs
// an installation or upgrade. It matches errors.Is(err, ErrIncompatibleSchema).
type CompatibilityError struct {
	// SchemaVersion is the installed schema version (the last migration
	// applied; FormatSchemaVersion gives its mq version); 0 when discovery is
	// missing or reports none.
	SchemaVersion int64
	// SupportedVersions are the mq lines this client supports
	// (SupportedMQVersions).
	SupportedVersions []string
	// Err is the database error when discovery is missing.
	Err error
}

func (e *CompatibilityError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("postgremq: postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports mq [%s]): %v",
			strings.Join(e.SupportedVersions, ", "), e.Err)
	}
	if e.SchemaVersion <= 0 {
		return fmt.Sprintf("postgremq: the database's PostgreMQ installation reports no schema version; it needs an installation or upgrade (client supports mq [%s])",
			strings.Join(e.SupportedVersions, ", "))
	}
	return fmt.Sprintf("postgremq: the database's PostgreMQ schema is mq %s (schema version %d); this client supports mq [%s]",
		formatSchemaVersion(uint64(e.SchemaVersion)), e.SchemaVersion, strings.Join(e.SupportedVersions, ", "))
}

// Unwrap exposes the database error when discovery is missing.
func (e *CompatibilityError) Unwrap() error { return e.Err }

// Is makes every CompatibilityError match ErrIncompatibleSchema.
func (e *CompatibilityError) Is(target error) bool { return target == ErrIncompatibleSchema }

// SQLSTATEs meaning the discovery function is not there: the function
// (undefined_function) or the whole schema (invalid_schema_name) is missing.
const (
	sqlStateUndefinedFunction = "42883"
	sqlStateInvalidSchemaName = "3F000"
)

// checkSchema reads postgremq.info() and rejects a schema whose mq line is
// not supported. Connection and permission errors are returned as they are.
func checkSchema(ctx context.Context, pool Pool) error {
	var raw []byte
	if err := pool.QueryRow(ctx, "SELECT postgremq.info()").Scan(&raw); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == sqlStateUndefinedFunction || pgErr.Code == sqlStateInvalidSchemaName) {
			return &CompatibilityError{SupportedVersions: SupportedMQVersions(), Err: err}
		}
		return fmt.Errorf("postgremq: read postgremq.info(): %w", err)
	}
	version, ok := schemaVersion(raw)
	if !ok {
		return &CompatibilityError{SupportedVersions: SupportedMQVersions()}
	}
	if !slices.Contains(supportedMQVersions, mqLine(version)) {
		return &CompatibilityError{SchemaVersion: version, SupportedVersions: SupportedMQVersions()}
	}
	return nil
}

// schemaVersion reads info()'s schema_version: a positive JSON integer. Any
// other value (null, missing, malformed) means the installation reports none.
func schemaVersion(raw []byte) (int64, bool) {
	var info struct {
		SchemaVersion json.RawMessage `json:"schema_version"`
	}
	if json.Unmarshal(raw, &info) != nil {
		return 0, false
	}
	version, err := strconv.ParseInt(string(info.SchemaVersion), 10, 64)
	return version, err == nil && version > 0
}
