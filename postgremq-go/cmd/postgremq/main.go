// Command postgremq installs and upgrades the PostgreMQ database schema.
//
//	postgremq migrate --dsn <connection-string>
//	postgremq status --dsn <connection-string>
//
// Install it with:
//
//	go install postgremq.dev/postgremq-go/cmd/postgremq@latest
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// command is one subcommand: its description and what it runs with the pool.
type command struct {
	summary string
	run     func(ctx context.Context, pool *pgxpool.Pool, out io.Writer) error
}

var commands = []struct {
	name string
	command
}{
	{"migrate", command{"Apply pending migrations up to the latest embedded version", runMigrate}},
	{"status", command{"Show the migration status", runStatus}},
}

// errUsage reports a command line error after its message was printed.
var errUsage = errors.New("usage")

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the command line and returns the exit code: 0 on success, 1
// on an error, 2 on a usage error.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	name := args[0]
	if name == "-h" || name == "--help" || name == "help" {
		usage(stdout)
		return 0
	}
	for _, c := range commands {
		if c.name != name {
			continue
		}
		err := runCommand(name, c.command, args[1:], stdout, stderr)
		switch {
		case err == nil:
			return 0
		case errors.Is(err, flag.ErrHelp):
			return 0
		case errors.Is(err, errUsage):
			return 2
		default:
			fmt.Fprintf(stderr, "Error: %v\n", err)
			return 1
		}
	}
	fmt.Fprintf(stderr, "Error: unknown command %q\n\n", name)
	usage(stderr)
	return 2
}

func runCommand(name string, c command, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("postgremq "+name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	dsn := flags.String("dsn", "", "Database connection string (required)")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: postgremq %s --dsn <connection-string>\n\n%s.\n\nFlags:\n", name, c.summary)
		flags.PrintDefaults()
	}
	// Help goes to stdout; usage errors to stderr.
	for _, arg := range args {
		if arg == "-h" || arg == "-help" || arg == "--help" {
			flags.SetOutput(stdout)
		}
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return errUsage
	}
	if flags.NArg() > 0 {
		fmt.Fprintf(stderr, "Error: unexpected arguments: %v\n\n", flags.Args())
		flags.Usage()
		return errUsage
	}
	if *dsn == "" {
		fmt.Fprintf(stderr, "Error: --dsn is required\n\n")
		flags.Usage()
		return errUsage
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer pool.Close()
	return c.run(ctx, pool, stdout)
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "postgremq manages the PostgreMQ database schema.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage: postgremq <command> --dsn <connection-string>")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, c := range commands {
		fmt.Fprintf(w, "  %-9s %s\n", c.name, c.summary)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run 'postgremq <command> -h' for the command's flags.")
}
