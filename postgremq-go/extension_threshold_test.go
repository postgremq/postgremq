// Tests for WithExtensionThreshold and the underlying calculateExtendAt math.
// Pure unit tests — no DB, no goroutines — so they run as part of every
// `go test` without testcontainer overhead.

package postgremq_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"postgremq.dev/postgremq-go"
)

func TestCalculateExtendAt_DefaultThreshold(t *testing.T) {
	// Default threshold (0.5) preserves the historical "halfway point"
	// behavior: with VT 60s in the future, extension fires ~30s out.
	c := postgremq.NewConsumerForTest(0.5)
	vtUntil := time.Now().Add(60 * time.Second)

	extendAt := c.CalculateExtendAt(vtUntil)

	expected := time.Now().Add(30 * time.Second)
	assert.WithinDuration(t, expected, extendAt, 100*time.Millisecond,
		"default threshold 0.5 should extend at ~50% of remaining VT")
}

func TestCalculateExtendAt_HigherThresholdExtendsLater(t *testing.T) {
	// threshold 0.8 — wait until 80% of remaining VT has elapsed before
	// extending. With VT 60s out, extension fires ~48s from now.
	c := postgremq.NewConsumerForTest(0.8)
	vtUntil := time.Now().Add(60 * time.Second)

	extendAt := c.CalculateExtendAt(vtUntil)

	expected := time.Now().Add(48 * time.Second)
	assert.WithinDuration(t, expected, extendAt, 100*time.Millisecond)
}

func TestCalculateExtendAt_LowerThresholdExtendsSooner(t *testing.T) {
	c := postgremq.NewConsumerForTest(0.2)
	vtUntil := time.Now().Add(60 * time.Second)

	extendAt := c.CalculateExtendAt(vtUntil)

	expected := time.Now().Add(12 * time.Second)
	assert.WithinDuration(t, expected, extendAt, 100*time.Millisecond)
}

func TestCalculateExtendAt_ExpiredVTReturnsNow(t *testing.T) {
	// If VT is already in the past (rare — only if a message took too
	// long to enter the extension queue), don't compute a negative
	// duration; just return now so the next loop iteration handles it.
	c := postgremq.NewConsumerForTest(0.5)
	vtUntil := time.Now().Add(-5 * time.Second)

	extendAt := c.CalculateExtendAt(vtUntil)

	assert.WithinDuration(t, time.Now(), extendAt, 100*time.Millisecond)
}

func TestValidateConsumeOptions_ExtensionThreshold(t *testing.T) {
	cases := []struct {
		name       string
		threshold  float64
		expectErr  bool
		errMessage string
	}{
		{"default (no override)", 0.5, false, ""},
		{"valid mid-range", 0.7, false, ""},
		{"valid near zero", 0.01, false, ""},
		{"valid near one", 0.99, false, ""},
		{"zero rejected", 0, true, "extension threshold"},
		{"one rejected", 1, true, "extension threshold"},
		{"negative rejected", -0.1, true, "extension threshold"},
		{"above one rejected", 1.5, true, "extension threshold"},
	}
	// `vt` must be > 0 for the other validation to pass — bake it into
	// every case so we exercise threshold validation in isolation.
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := postgremq.ValidateConsumeOptionsForTest(
				postgremq.WithVT(30),
				postgremq.WithExtensionThreshold(c.threshold),
			)
			if c.expectErr {
				require.Error(t, err)
				assert.True(t,
					strings.Contains(err.Error(), c.errMessage),
					"error %q should mention %q", err, c.errMessage)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
