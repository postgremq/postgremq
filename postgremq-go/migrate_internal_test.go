package postgremq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationLockIDMatchesGolangMigrate(t *testing.T) {
	// golang-migrate's database.GenerateAdvisoryLockId(db, "postgremq", "postgremq_migrations").
	for database, want := range map[string]int64{
		"postgres": 2_735_559_060,
		"mydb":     1_257_508_284,
		"pmq_ts_1": 3_263_720_984,
	} {
		assert.Equal(t, want, migrationLockID(database), database)
	}
}

func TestEmbeddedMigrationsAreAllReadInOrder(t *testing.T) {
	entries, err := migrationsFS.ReadDir("migrations")
	require.NoError(t, err)
	migrations, err := loadMigrations()
	require.NoError(t, err)
	require.Len(t, migrations, len(entries))
	for i, m := range migrations {
		name := entries[i].Name()
		assert.Equal(t, name, fmt.Sprintf("%09d_%s.up.sql", m.version, formatSchemaVersion(m.version)), "read in order")
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		require.NoError(t, err)
		assert.Equal(t, string(sql), m.sql, name)
	}
	assert.Equal(t, uint(migrations[len(migrations)-1].version), getLatestMigrationVersion())
}

func TestSQLPositionCountsCharactersFromOne(t *testing.T) {
	sql := "ab\nc☕d\n"
	for position, want := range map[int][2]int{
		1: {1, 1}, // a
		3: {1, 3}, // the first newline
		4: {2, 1}, // c
		6: {2, 3}, // d, after a 3-byte character
		7: {2, 4}, // the last newline
		8: {3, 1}, // the end of the input
	} {
		line, column, ok := sqlPosition(sql, position, positionsInCharacters)
		require.True(t, ok, position)
		assert.Equal(t, want, [2]int{line, column}, position)
	}
	for _, position := range []int{0, -1, 9} {
		_, _, ok := sqlPosition(sql, position, positionsInCharacters)
		assert.False(t, ok, position)
	}
	line, column, ok := sqlPosition("SELECT (", 9, positionsInCharacters)
	assert.Equal(t, [2]int{1, 9}, [2]int{line, column}, "the end of input without a newline")
	assert.True(t, ok)
}

// A single-byte server encoding counts the UTF-8 file's bytes; the column is
// still in characters.
func TestSQLPositionInBytes(t *testing.T) {
	sql := "ab\nc☕d\n"
	for position, want := range map[int][2]int{
		4:  {2, 1}, // c
		8:  {2, 3}, // d, after a 3-byte character
		9:  {2, 4}, // the last newline
		10: {3, 1}, // the end of the input
	} {
		line, column, ok := sqlPosition(sql, position, positionsInBytes)
		require.True(t, ok, position)
		assert.Equal(t, want, [2]int{line, column}, position)
	}
	_, _, ok := sqlPosition(sql, 11, positionsInBytes)
	assert.False(t, ok)
	_, _, ok = sqlPosition(sql, 4, positionsUnmapped)
	assert.False(t, ok, "unmapped")
}

// Measured on PostgreSQL 15 for every pair of these encodings. EUC_JP and
// SJIS count characters of their own, neither the UTF-8 text's characters
// nor its bytes; UTF8 into EUC_JIS_2004 combines か and ゚ into one.
func TestPositionUnitFollowsTheServersConversion(t *testing.T) {
	for _, tc := range []struct {
		server, client string
		unit           positionUnit
	}{
		{"UTF8", "UTF8", positionsInCharacters},
		{"UTF8", "SQL_ASCII", positionsInCharacters},
		{"UTF8", "LATIN1", positionsInBytes},
		{"UTF8", "EUC_JP", positionsUnmapped},
		{"UTF8", "SJIS", positionsUnmapped},
		{"SQL_ASCII", "UTF8", positionsInBytes},
		{"SQL_ASCII", "SQL_ASCII", positionsInBytes},
		{"SQL_ASCII", "LATIN1", positionsInBytes},
		{"LATIN1", "UTF8", positionsInCharacters},
		{"LATIN1", "SQL_ASCII", positionsInBytes},
		{"LATIN1", "LATIN1", positionsInBytes},
		{"EUC_JP", "UTF8", positionsInCharacters},
		{"EUC_JP", "EUC_JP", positionsUnmapped},
		{"EUC_JP", "SQL_ASCII", positionsUnmapped},
		{"EUC_JP", "SJIS", positionsUnmapped},
		{"EUC_JIS_2004", "UTF8", positionsUnmapped},
		{"WIN1251", "UNICODE", positionsInCharacters},
	} {
		assert.Equal(t, tc.unit, positionUnitOf(tc.server, tc.client), "%s/%s", tc.server, tc.client)
	}
}
