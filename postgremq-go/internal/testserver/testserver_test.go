package testserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseURLReplacesOnlyTheDatabase(t *testing.T) {
	for in, want := range map[string]string{
		"postgres://u:p@localhost:5432/postgres?sslmode=disable": "postgres://u:p@localhost:5432/pmq_1?sslmode=disable",
		"postgresql://u@db/": "postgresql://u@db/pmq_1",
		"postgres://u@db":    "postgres://u@db/pmq_1",
		"postgres://u@db/postgres?dbname=postgres&sslmode=disable":   "postgres://u@db/pmq_1?sslmode=disable",
		"postgres://u@db/postgres?database=postgres":                 "postgres://u@db/pmq_1",
		"postgres:///postgres?host=%2Fvar%2Frun%2Fpostgresql&user=u": "postgres:///pmq_1?host=%2Fvar%2Frun%2Fpostgresql&user=u",
		"postgres://u:p@/postgres?host=%2Ftmp":                       "postgres://u:p@/pmq_1?host=%2Ftmp",
	} {
		got, err := DatabaseURL(in, "pmq_1")
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
}

func TestDatabaseURLWantsAPostgresURL(t *testing.T) {
	for _, in := range []string{"host=localhost user=u password=secret dbname=postgres", "mysql://u:secret@db/x"} {
		_, err := DatabaseURL(in, "pmq_1")
		require.Error(t, err, in)
		assert.NotContains(t, err.Error(), "secret", "the error must not echo a password")
	}
}
