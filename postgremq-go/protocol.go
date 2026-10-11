package postgremq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgconn"
)

// supportedProtocolMajors are the PostgreMQ protocol majors this client
// implements and is tested against.
var supportedProtocolMajors = []int{1}

// SupportedProtocolMajors returns the PostgreMQ protocol majors this client
// implements. Dial and DialFromPool reject a database whose postgremq.info()
// reports any other major.
func SupportedProtocolMajors() []int { return slices.Clone(supportedProtocolMajors) }

// ErrIncompatibleSchema matches every *CompatibilityError.
var ErrIncompatibleSchema = errors.New("postgremq: incompatible database installation")

// CompatibilityError is returned by Dial and DialFromPool when the database's
// PostgreMQ installation does not speak a protocol major this client supports,
// or has no discovery function (postgremq.info()) and needs an installation or
// upgrade. It matches errors.Is(err, ErrIncompatibleSchema).
type CompatibilityError struct {
	// SchemaVersion is the installed schema version (the number of the last
	// migration applied); 0 when discovery is missing or reports none.
	SchemaVersion int64
	// ProtocolMajor is the installation's protocol major; 0 when discovery
	// is missing.
	ProtocolMajor int
	// SupportedMajors are the protocol majors this client supports.
	SupportedMajors []int
	// Err is the database error when discovery is missing.
	Err error
}

func (e *CompatibilityError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("postgremq: postgremq.info() is unavailable; the database needs a PostgreMQ installation or upgrade (client supports protocol majors %v): %v",
			e.SupportedMajors, e.Err)
	}
	return fmt.Sprintf("postgremq: the database's PostgreMQ schema (version %d) uses protocol major %d; this client supports %v",
		e.SchemaVersion, e.ProtocolMajor, e.SupportedMajors)
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

// checkProtocol reads postgremq.info() and rejects an unsupported protocol
// major. Connection and permission errors are returned as they are.
func checkProtocol(ctx context.Context, pool Pool) error {
	var raw []byte
	if err := pool.QueryRow(ctx, "SELECT postgremq.info()").Scan(&raw); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == sqlStateUndefinedFunction || pgErr.Code == sqlStateInvalidSchemaName) {
			return &CompatibilityError{SupportedMajors: SupportedProtocolMajors(), Err: err}
		}
		return fmt.Errorf("postgremq: read postgremq.info(): %w", err)
	}
	var info struct {
		SchemaVersion int64 `json:"schema_version"`
		ProtocolMajor int   `json:"protocol_major"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return fmt.Errorf("postgremq: decode postgremq.info(): %w", err)
	}
	if !slices.Contains(supportedProtocolMajors, info.ProtocolMajor) {
		return &CompatibilityError{
			SchemaVersion:   info.SchemaVersion,
			ProtocolMajor:   info.ProtocolMajor,
			SupportedMajors: SupportedProtocolMajors(),
		}
	}
	return nil
}
