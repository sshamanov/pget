// pget is a Wget-style command-line downloader with parallel chunked retrieval
// and ordered streaming.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sshamanov/pget/internal/cli"
	"github.com/sshamanov/pget/internal/job"
)

func main() {
	// Parse CLI arguments.
	plan, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "pget: %v\n", err)
		os.Exit(int(job.ExitParseError))
	}

	// Background mode: parent spawns child and exits.
	if plan.Background == cli.BackgroundChild && !job.IsBackgroundChild() {
		if err := job.DetachToBackground(); err != nil {
			fmt.Fprintf(os.Stderr, "pget: %v\n", err)
			os.Exit(int(job.ExitGenericError))
		}
		// DetachToBackground exits; this line is only reached by the child.
	}

	// Set up signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Create and run the job runner.
	runner := job.NewRunner(plan)
	exitCode := runner.Run(ctx)

	os.Exit(int(exitCode))
}
