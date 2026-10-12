package postgremq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
)

// supportedMQMajors are the mq (schema) majors this client implements and is
// tested against. A breaking schema change bumps the mq major.
var supportedMQMajors = []int{0}

// SupportedMQMajors returns the mq (schema) majors this client implements.
// Dial and DialFromPool reject a database whose schema version, as
// postgremq.info() reports it, has any other major.
func SupportedMQMajors() []int { return slices.Clone(supportedMQMajors) }

// ErrIncompatibleSchema matches every *CompatibilityError.
var ErrIncompatibleSchema = errors.New("postgremq: incompatible database installation")

// CompatibilityError is returned by Dial and DialFromPool when the database's
// PostgreMQ schema has an mq major this client does not support, reports no
// schema version, or has no discovery function (postgremq.info()) and needs
// an installation or upgrade. It matches errors.Is(err, ErrIncompatibleSchema).
type CompatibilityError struct {
	// SchemaVersion is the installed schema version (the last migration
	// applied; FormatSchemaVersion gives its mq version); 0 when discovery is
	// missing or reports none.
	SchemaVersion int64
	// SupportedMajors are the mq majors this client supports.
	SupportedMajors []int
	// Err is the database error when discovery is missing.
	Err error
}

func (e *CompatibilityError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("postgremq: postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports mq majors %v): %v",
			e.SupportedMajors, e.Err)
	}
	if e.SchemaVersion <= 0 {
		return fmt.Sprintf("postgremq: the database's PostgreMQ installation reports no schema version; it needs an installation or upgrade (client supports mq majors %v)",
			e.SupportedMajors)
	}
	return fmt.Sprintf("postgremq: the database's PostgreMQ schema is mq %s (schema version %d); this client supports mq majors %v",
		formatSchemaVersion(uint64(e.SchemaVersion)), e.SchemaVersion, e.SupportedMajors)
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

// checkSchema reads postgremq.info() and rejects a schema whose mq major is
// not supported. Connection and permission errors are returned as they are.
func checkSchema(ctx context.Context, pool Pool) error {
	var raw []byte
	if err := pool.QueryRow(ctx, "SELECT postgremq.info()").Scan(&raw); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == sqlStateUndefinedFunction || pgErr.Code == sqlStateInvalidSchemaName) {
			return &CompatibilityError{SupportedMajors: SupportedMQMajors(), Err: err}
		}
		return fmt.Errorf("postgremq: read postgremq.info(): %w", err)
	}
	version, ok := schemaVersion(raw)
	if !ok {
		return &CompatibilityError{SupportedMajors: SupportedMQMajors()}
	}
	if !slices.Contains(supportedMQMajors, int(version/100_000_000)) {
		return &CompatibilityError{SchemaVersion: version, SupportedMajors: SupportedMQMajors()}
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
