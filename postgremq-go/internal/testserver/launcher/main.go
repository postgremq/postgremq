// Command launcher starts the postgres:15 container the postgremq-go tests
// use when POSTGREMQ_TEST_DATABASE_URL is unset (see package testserver). It
// prints the container's URL on stdout as one line,
//
//	POSTGREMQ_TEST_DATABASE_URL=postgres://...
//
// after a first line, POSTGREMQ_TEST_LAUNCHER_RUNNING, printed as soon as it
// runs (the test process then deletes the launcher's binary), and removes
// the container when its stdin reaches EOF (the test process
// closed it or exited, however it exited) or it receives SIGINT or SIGTERM.
// Everything else it writes goes to stderr; with -v, that includes
// testcontainers-go's log. If the launcher itself is killed,
// testcontainers' reaper (Ryuk) removes the container.
//
// It is a separate module so that testcontainers-go and the Docker client
// stay out of postgremq-go's go.mod.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "launcher:", err)
		os.Exit(1)
	}
}

func run() (err error) {
	// testcontainers-go logs only in a verbose test binary; the test process
	// passes -v when its tests run verbose.
	if len(os.Args) > 1 && os.Args[1] == "-v" {
		testcontainers.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	// The test process reads the URL from stdout: keep everything else
	// (testcontainers' logs) off it.
	out := os.Stdout
	os.Stdout = os.Stderr
	if _, err := fmt.Fprintln(out, "POSTGREMQ_TEST_LAUNCHER_RUNNING"); err != nil {
		return err
	}
	// After the test process exits, a write to its stderr pipe must fail
	// instead of killing the launcher before it removes the container.
	signal.Ignore(syscall.SIGPIPE)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		stop()
	}()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:15",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
				"POSTGRES_DB":       "postgres",
			},
			Cmd: []string{
				"postgres",
				"-c", "max_connections=200", // Increase max connections for parallel tests
			},
			WaitingFor: wait.ForAll(
				// The entrypoint restarts the server once after initdb.
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				// Docker Desktop on macOS and Windows serves the port through
				// a proxy that may start later.
				wait.ForListeningPort("5432/tcp"),
			).WithDeadline(30 * time.Second),
		},
		Started: true,
	})
	if container != nil {
		defer func() {
			terminateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if terr := container.Terminate(terminateCtx); terr != nil && err == nil {
				err = fmt.Errorf("remove the container: %w", terr)
			}
		}()
	}
	if err != nil {
		return fmt.Errorf("start postgres:15 (is Docker running?): %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return fmt.Errorf("container host: %w", err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return fmt.Errorf("container port: %w", err)
	}
	if _, err := fmt.Fprintf(out, "POSTGREMQ_TEST_DATABASE_URL=postgres://postgres:postgres@%s/postgres?sslmode=disable\n",
		net.JoinHostPort(host, port.Port())); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}
