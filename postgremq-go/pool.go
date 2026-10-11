package postgremq

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool defines the minimal database operations the client depends on.
//
// This interface abstracts the database connection pool to allow for testing
// with mocks and fakes. The primary implementation is *pgxpool.Pool from the
// jackc/pgx library.
//
// All methods on this interface are safe to call from multiple goroutines
// concurrently (when using the standard pgxpool.Pool implementation).
type Pool interface {
	// Exec executes a SQL command and returns the command tag.
	//
	// The command tag contains information such as the number of rows affected
	// by INSERT, UPDATE, or DELETE commands.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control.
	//   - sql: SQL command to execute.
	//   - arguments: Arguments to bind to the SQL command using positional parameters ($1, $2, etc.).
	//
	// Returns the command tag and an error if the operation fails.
	//
	// This method is used by Connection for operations like CreateTopic, CreateQueue,
	// DeleteTopic, etc.
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)

	// Query executes a SQL query that returns multiple rows.
	//
	// The caller MUST close the returned Rows by calling rows.Close() to release
	// the database connection back to the pool. Failure to close Rows will leak
	// connections.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control.
	//   - sql: SQL query to execute.
	//   - args: Arguments to bind to the SQL query using positional parameters ($1, $2, etc.).
	//
	// Returns Rows for iteration and an error if the query fails.
	//
	// Example:
	//   rows, err := pool.Query(ctx, "SELECT * FROM postgremq.topics")
	//   if err != nil { return err }
	//   defer rows.Close()
	//   for rows.Next() { ... }
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)

	// QueryRow executes a SQL query that is expected to return at most one row.
	//
	// Unlike Query, QueryRow does not return an error directly. Instead, any error
	// encountered is deferred until the Row's Scan method is called.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control.
	//   - sql: SQL query to execute.
	//   - args: Arguments to bind to the SQL query using positional parameters ($1, $2, etc.).
	//
	// Returns a Row that can be scanned for the query result.
	//
	// This method is used extensively by Connection for operations that return a single
	// value, such as Publish (returns message ID) or GetQueueStatistics (returns counts).
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	// Acquire obtains a dedicated connection from the pool.
	//
	// The caller MUST call Release() on the returned connection when done to return
	// it to the pool. Failure to release connections will exhaust the pool.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control.
	//
	// Returns a pooled connection and an error if acquisition fails (e.g., pool is closed,
	// context cancelled, or all connections are in use and max pool size is reached).
	//
	// This method is used internally by EventListener to establish a dedicated connection
	// for PostgreSQL LISTEN/NOTIFY.
	//
	// Example:
	//   conn, err := pool.Acquire(ctx)
	//   if err != nil { return err }
	//   defer conn.Release()
	//   // Use conn.Exec, conn.Query, etc.
	Acquire(ctx context.Context) (*pgxpool.Conn, error)

	// Close closes all connections in the pool and releases associated resources.
	//
	// After Close is called, all subsequent operations on the Pool will fail.
	// This method waits for all acquired connections to be released before returning.
	//
	// This method is called by Connection.Close() if the Connection owns the pool
	// (created via Dial). If the pool was provided via DialFromPool, Close is NOT
	// called, leaving pool lifetime management to the caller.
	Close()
}

// Tx defines the minimal transaction interface used by this package.
//
// This is a subset of pgx.Tx that includes only methods required by
// PublishWithTx and AckWithTx code paths. Implementations should typically
// use pgx.Tx or a compatible transaction type.
//
// Important: Methods on this interface do NOT use the retry policy since
// transaction boundaries are controlled by the caller.
type Tx interface {
	// Exec executes a SQL command within the transaction and returns the command tag.
	//
	// The command tag contains information such as the number of rows affected.
	// If the transaction is rolled back, all effects of this command are undone.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control.
	//   - sql: SQL command to execute.
	//   - arguments: Arguments to bind to the SQL command.
	//
	// Returns the command tag and an error if the operation fails.
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)

	// QueryRow executes a SQL query within the transaction that is expected to return at most one row.
	//
	// The returned Row allows scanning the result or checking for errors.
	// If the transaction is rolled back, the query results become invalid.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout control.
	//   - sql: SQL query to execute.
	//   - args: Arguments to bind to the SQL query.
	//
	// Returns a Row that can be scanned for the query result.
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
