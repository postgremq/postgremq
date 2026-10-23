// Package testserver gives the module's tests a PostgreSQL server.
//
// The server is POSTGREMQ_TEST_DATABASE_URL when it is set: a postgres://
// URL of a PostgreSQL 15+ server whose user can create databases and roles
// (and max_connections of 200 or more, as the tests run in parallel). The
// tests connect to the URL's database to create and drop theirs, so it must
// exist (normally /postgres); a Unix socket goes in ?host=/path.
// Otherwise Start builds and runs the launcher in the launcher directory next
// to this file, a separate module that starts a postgres:15 container with
// testcontainers-go, so that neither it nor the Docker client is in this
// module's go.mod. The launcher removes the container when its stdin closes:
// on Close, or when the test process exits however it exits. It needs the
// repository: the module zip leaves the launcher module out.
package testserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// EnvURL names the environment variable with the URL of a server to use
// instead of starting a container.
const EnvURL = "POSTGREMQ_TEST_DATABASE_URL"

// startTimeout bounds the container start, including the first image pull.
const startTimeout = 5 * time.Minute

// Server is the PostgreSQL server the tests use.
type Server struct {
	// URL is a postgres:// URL of the server's admin database.
	URL string

	launcher *exec.Cmd
	stdin    io.Closer
	exited   chan error // receives the launcher's exit, then closes
	binDir   string
}

// Start returns POSTGREMQ_TEST_DATABASE_URL's server if it is set, otherwise
// a new postgres:15 container; Close removes the container.
func Start(ctx context.Context) (*Server, error) {
	if dsn := os.Getenv(EnvURL); dsn != "" {
		if _, err := DatabaseURL(dsn, "postgres"); err != nil {
			return nil, fmt.Errorf("%s: %w", EnvURL, err)
		}
		return &Server{URL: dsn}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	binDir, err := os.MkdirTemp("", "postgremq-test-launcher")
	if err != nil {
		return nil, err
	}
	bin, err := buildLauncher(ctx, binDir)
	if err != nil {
		_ = os.RemoveAll(binDir)
		return nil, err
	}

	var args []string
	if verbose() {
		args = append(args, "-v")
	}
	cmd := exec.Command(bin, args...)
	// Close's kill fallback must not wait for pipes a launcher's child holds.
	cmd.WaitDelay = 10 * time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = os.RemoveAll(binDir)
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = os.RemoveAll(binDir)
		return nil, err
	}
	// Not *os.File: the launcher writes to a pipe this process copies from,
	// so it never holds `go test`'s own stderr open after this process exits.
	cmd.Stderr = struct{ io.Writer }{os.Stderr}
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(binDir)
		return nil, fmt.Errorf("run the test-server launcher: %w", err)
	}
	s := &Server{launcher: cmd, stdin: stdin, exited: make(chan error, 1), binDir: binDir}

	found := make(chan string, 1)
	go func() {
		const prefix = EnvURL + "="
		lines := bufio.NewReader(stdout)
		sent := false
		for {
			line, err := lines.ReadString('\n')
			if line == "POSTGREMQ_TEST_LAUNCHER_RUNNING\n" {
				// The launcher is past exec, so its binary can go now (macOS
				// sometimes kills a process whose executable is deleted while
				// it starts), and a test run killed while the container starts
				// leaves nothing behind. Windows keeps a running executable;
				// Close removes it there.
				_ = os.RemoveAll(binDir)
			} else if !sent && strings.HasPrefix(line, prefix) && err == nil {
				found <- strings.TrimSpace(strings.TrimPrefix(line, prefix))
				sent = true
			} else if line != "" {
				fmt.Fprint(os.Stderr, line)
			}
			if err != nil {
				break
			}
		}
		s.exited <- cmd.Wait()
		close(s.exited)
	}()

	select {
	case s.URL = <-found:
		select {
		case err := <-s.exited:
			_ = os.RemoveAll(binDir)
			return nil, fmt.Errorf("the test-server launcher exited right after it started the server (%v); its output is above", err)
		default:
		}
		return s, nil
	case err := <-s.exited:
		_ = os.RemoveAll(binDir)
		return nil, fmt.Errorf("the test-server launcher exited before the server started (%v); its output is above. "+
			"Docker must be running, or set %s", err, EnvURL)
	case <-ctx.Done():
		_ = s.Close()
		return nil, fmt.Errorf("the test server did not start within %v: %w", startTimeout, ctx.Err())
	}
}

// Close removes the container Start started, if any.
func (s *Server) Close() error {
	if s.launcher == nil {
		return nil
	}
	defer os.RemoveAll(s.binDir)
	_ = s.stdin.Close()
	select {
	case err := <-s.exited:
		return err
	case <-time.After(time.Minute):
		_ = s.launcher.Process.Kill()
		<-s.exited
		return errors.New("the test-server launcher did not stop within a minute; killed it (testcontainers' reaper removes its container)")
	}
}

// DatabaseURL returns serverURL with its database replaced by database.
func DatabaseURL(serverURL, database string) (string, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return "", errors.New("want a postgres:// URL") // err would echo a password
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return "", fmt.Errorf("want a postgres:// URL, got scheme %q", u.Scheme)
	}
	u.Path = "/" + database
	u.RawPath = ""
	// pgx lets these query parameters override the path.
	query := u.Query()
	query.Del("dbname")
	query.Del("database")
	u.RawQuery = query.Encode()
	return u.String(), nil
}

// buildLauncher builds the launcher module into dir. It is built rather than
// run with `go run`, so that the launcher is this process's own child.
func buildLauncher(ctx context.Context, dir string) (string, error) {
	src, err := launcherSource()
	if err != nil {
		return "", err
	}
	bin := filepath.Join(dir, "launcher")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, ".")
	build.Dir = src
	// The launcher is its own module, outside the repository's go.work.
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build the test-server launcher in %s: %w\n%s", src, err, out)
	}
	return bin, nil
}

// launcherSource finds the launcher module above the working directory (a
// test runs in its package's directory).
func launcherSource() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		src := filepath.Join(dir, "internal", "testserver", "launcher")
		if _, err := os.Stat(filepath.Join(src, "go.mod")); err == nil {
			return src, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("the test-server launcher is not above %s (the module zip leaves it out); set %s", dir, EnvURL)
		}
		dir = parent
	}
}

// verbose reports whether the tests run with -v; the launcher then logs as
// testcontainers-go does in a verbose test.
func verbose() bool {
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-test.v=") && arg != "-test.v=false" {
			return true
		}
	}
	return false
}
