package postgremq

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every module in the build list is in go.mod (go >= 1.17), and all of them
// take part in every consumer's version selection although none of these is
// built: golang-migrate brings every database driver's requirements (thrift,
// grpc, ...), testcontainers-go the Docker client's. The tests start their
// container through a separate module (internal/testserver/launcher).
func TestModuleRequiresNoMigrationOrContainerTooling(t *testing.T) {
	out, err := exec.Command("go", "mod", "edit", "-json").Output()
	require.NoError(t, err)
	var mod struct{ Require []struct{ Path string } }
	require.NoError(t, json.Unmarshal(out, &mod))
	require.NotEmpty(t, mod.Require)
	for _, req := range mod.Require {
		for _, banned := range []string{
			"github.com/golang-migrate/",
			"github.com/testcontainers/",
			"github.com/docker/",
			"github.com/apache/thrift",
			"google.golang.org/grpc",
		} {
			assert.False(t, strings.HasPrefix(req.Path, banned), "go.mod requires %s", req.Path)
		}
	}
}
