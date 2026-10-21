package postgremq

import "testing"

func TestMQLine(t *testing.T) {
	for version, want := range map[int64]string{
		1:           "0.0",
		10_000:      "0.1",
		19_999:      "0.1",
		20_000:      "0.2",
		99_999_999:  "0.9999",
		100_000_000: "1",
		100_020_003: "1",
		299_990_000: "2",
	} {
		if got := mqLine(version); got != want {
			t.Errorf("mqLine(%d) = %q, want %q", version, got, want)
		}
	}
}
