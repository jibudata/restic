package main

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	internalchunker "github.com/restic/restic/internal/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/filter"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/restorer"
	"github.com/restic/restic/internal/ui"
	restoreui "github.com/restic/restic/internal/ui/restore"
	"github.com/restic/restic/internal/ui/termstatus"
	"gopkg.in/yaml.v3"

	"github.com/spf13/cobra"
)

var cmdRestore = &cobra.Command{
	Use:   "restore [flags] snapshotID",
	Short: "Extract the data from a snapshot",
	Long: `
The "restore" command extracts the data from a snapshot from the repository to
a directory.

The special snapshotID "latest" can be used to restore the latest snapshot in the
repository.

To only restore a specific subfolder, you can use the "<snapshotID>:<subfolder>"
syntax, where "subfolder" is a path within the snapshot.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		var wg sync.WaitGroup
		cancelCtx, cancel := context.WithCancel(ctx)
		defer func() {
			// shutdown termstatus
			cancel()
			wg.Wait()
		}()

		term := termstatus.New(globalOptions.stdout, globalOptions.stderr, globalOptions.Quiet)
		wg.Add(1)
		go func() {
			defer wg.Done()
			term.Run(cancelCtx)
		}()

		// allow usage of warnf / verbosef
		prevStdout, prevStderr := globalOptions.stdout, globalOptions.stderr
		defer func() {
			globalOptions.stdout, globalOptions.stderr = prevStdout, prevStderr
		}()
		stdioWrapper := ui.NewStdioWrapper(term)
		globalOptions.stdout, globalOptions.stderr = stdioWrapper.Stdout(), stdioWrapper.Stderr()

		return runRestore(ctx, restoreOptions, globalOptions, term, args)
	},
}

// RestoreOptions collects all options for the restore command.
type RestoreOptions struct {
	Exclude            []string
	InsensitiveExclude []string
	Include            []string
	InsensitiveInclude []string
	Target             string
	restic.SnapshotFilter
	Sparse           bool
	Verify           bool
	SkipExisting     bool
	ReChunk          bool
	QuickChangeCheck bool
	ChunkFile        string
}

var restoreOptions RestoreOptions

func init() {
	cmdRoot.AddCommand(cmdRestore)

	flags := cmdRestore.Flags()
	flags.StringArrayVarP(&restoreOptions.Exclude, "exclude", "e", nil, "exclude a `pattern` (can be specified multiple times)")
	flags.StringArrayVar(&restoreOptions.InsensitiveExclude, "iexclude", nil, "same as --exclude but ignores the casing of `pattern`")
	flags.StringArrayVarP(&restoreOptions.Include, "include", "i", nil, "include a `pattern`, exclude everything else (can be specified multiple times)")
	flags.StringArrayVar(&restoreOptions.InsensitiveInclude, "iinclude", nil, "same as --include but ignores the casing of `pattern`")
	flags.StringVarP(&restoreOptions.Target, "target", "t", "", "directory to extract data to")

	initSingleSnapshotFilter(flags, &restoreOptions.SnapshotFilter)
	flags.BoolVar(&restoreOptions.Sparse, "sparse", false, "restore files as sparse")
	flags.BoolVar(&restoreOptions.Verify, "verify", false, "verify restored files content")
	flags.BoolVar(&restoreOptions.SkipExisting, "skip-existing", false, "skip restoring file blobs that exist on disk")
	flags.BoolVar(&restoreOptions.ReChunk, "rechunk", false, "divide files on disk into chunks and compare these chunks with corresponding chunks from the same file in the snapshot. If this flag is not set, the chunk sizes from the snapshot files will be used to divide the files on disk for comparison. It should be used together with --skip-existing")
	flags.BoolVar(&restoreOptions.QuickChangeCheck, "quick-change-check", false, "check if file has changed by comparing size and mtime, it should be used together with --skip-existing")
	flags.StringVar(&restoreOptions.ChunkFile, "chunk-file", "", "file to read file chunks from, instead of calculating them, it should be used together with --skip-existing, and should not be used with --rechunk")
}

func runRestore(ctx context.Context, opts RestoreOptions, gopts GlobalOptions,
	term *termstatus.Terminal, args []string) error {

	hasExcludes := len(opts.Exclude) > 0 || len(opts.InsensitiveExclude) > 0
	hasIncludes := len(opts.Include) > 0 || len(opts.InsensitiveInclude) > 0

	// Validate provided patterns
	if len(opts.Exclude) > 0 {
		if err := filter.ValidatePatterns(opts.Exclude); err != nil {
			return errors.Fatalf("--exclude: %s", err)
		}
	}
	if len(opts.InsensitiveExclude) > 0 {
		if err := filter.ValidatePatterns(opts.InsensitiveExclude); err != nil {
			return errors.Fatalf("--iexclude: %s", err)
		}
	}
	if len(opts.Include) > 0 {
		if err := filter.ValidatePatterns(opts.Include); err != nil {
			return errors.Fatalf("--include: %s", err)
		}
	}
	if len(opts.InsensitiveInclude) > 0 {
		if err := filter.ValidatePatterns(opts.InsensitiveInclude); err != nil {
			return errors.Fatalf("--iinclude: %s", err)
		}
	}

	for i, str := range opts.InsensitiveExclude {
		opts.InsensitiveExclude[i] = strings.ToLower(str)
	}

	for i, str := range opts.InsensitiveInclude {
		opts.InsensitiveInclude[i] = strings.ToLower(str)
	}

	switch {
	case len(args) == 0:
		return errors.Fatal("no snapshot ID specified")
	case len(args) > 1:
		return errors.Fatalf("more than one snapshot ID specified: %v", args)
	}

	if opts.Target == "" {
		return errors.Fatal("please specify a directory to restore to (--target)")
	}

	if hasExcludes && hasIncludes {
		return errors.Fatal("exclude and include patterns are mutually exclusive")
	}

	snapshotIDString := args[0]

	debug.Log("restore %v to %v", snapshotIDString, opts.Target)

	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}

	if !gopts.NoLock {
		var lock *restic.Lock
		lock, ctx, err = lockRepo(ctx, repo, gopts.RetryLock, gopts.JSON)
		defer unlockRepo(lock)
		if err != nil {
			return err
		}
	}

	sn, subfolder, err := (&restic.SnapshotFilter{
		Hosts: opts.Hosts,
		Paths: opts.Paths,
		Tags:  opts.Tags,
	}).FindLatest(ctx, repo.Backend(), repo, snapshotIDString)
	if err != nil {
		return errors.Fatalf("failed to find snapshot: %v", err)
	}

	bar := newIndexTerminalProgress(gopts.Quiet, gopts.JSON, term)
	err = repo.LoadIndex(ctx, bar)
	if err != nil {
		return err
	}

	sn.Tree, err = restic.FindTreeDirectory(ctx, repo, sn.Tree, subfolder)
	if err != nil {
		return err
	}

	msg := ui.NewMessage(term, gopts.verbosity)
	var printer restoreui.ProgressPrinter
	if gopts.JSON {
		printer = restoreui.NewJSONProgress(term)
	} else {
		printer = restoreui.NewTextProgress(term)
	}

	var fileChunkInfos *internalchunker.FileChunkInfoMap
	if opts.ChunkFile != "" {
		data, err := os.ReadFile(opts.ChunkFile)
		if err != nil {
			return err
		} else if len(data) > 0 {
			fileChunkInfos = &internalchunker.FileChunkInfoMap{}
			err = yaml.Unmarshal(data, fileChunkInfos)
			if err != nil {
				return err
			}
		}
	}

	progress := restoreui.NewProgress(printer, calculateProgressInterval(!gopts.Quiet, gopts.JSON))
	res := restorer.NewRestorer(repo, sn, opts.Sparse, progress,
		restorer.WithSkipExisting(opts.SkipExisting),
		restorer.WithReChunk(opts.ReChunk),
		restorer.WithQuickChangeCheck(opts.QuickChangeCheck),
		restorer.WithFileChunkInfos(fileChunkInfos),
	)

	totalErrors := 0
	res.Error = func(location string, err error) error {
		msg.E("ignoring error for %s: %s\n", location, err)
		totalErrors++
		return nil
	}

	excludePatterns := filter.ParsePatterns(opts.Exclude)
	insensitiveExcludePatterns := filter.ParsePatterns(opts.InsensitiveExclude)
	selectExcludeFilter := func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
		matched, err := filter.List(excludePatterns, item)
		if err != nil {
			msg.E("error for exclude pattern: %v", err)
		}

		matchedInsensitive, err := filter.List(insensitiveExcludePatterns, strings.ToLower(item))
		if err != nil {
			msg.E("error for iexclude pattern: %v", err)
		}

		// An exclude filter is basically a 'wildcard but foo',
		// so even if a childMayMatch, other children of a dir may not,
		// therefore childMayMatch does not matter, but we should not go down
		// unless the dir is selected for restore
		selectedForRestore = !matched && !matchedInsensitive
		childMayBeSelected = selectedForRestore && node.Type == "dir"

		return selectedForRestore, childMayBeSelected
	}

	includePatterns := filter.ParsePatterns(opts.Include)
	insensitiveIncludePatterns := filter.ParsePatterns(opts.InsensitiveInclude)
	selectIncludeFilter := func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
		matched, childMayMatch, err := filter.ListWithChild(includePatterns, item)
		if err != nil {
			msg.E("error for include pattern: %v", err)
		}

		matchedInsensitive, childMayMatchInsensitive, err := filter.ListWithChild(insensitiveIncludePatterns, strings.ToLower(item))
		if err != nil {
			msg.E("error for iexclude pattern: %v", err)
		}

		selectedForRestore = matched || matchedInsensitive
		childMayBeSelected = (childMayMatch || childMayMatchInsensitive) && node.Type == "dir"

		return selectedForRestore, childMayBeSelected
	}

	if hasExcludes {
		res.SelectFilter = selectExcludeFilter
	} else if hasIncludes {
		res.SelectFilter = selectIncludeFilter
	}

	if !gopts.JSON {
		msg.P("restoring %s to %s\n", res.Snapshot(), opts.Target)
	}

	err = res.RestoreTo(ctx, opts.Target)
	if err != nil {
		return err
	}

	progress.Finish()

	if totalErrors > 0 {
		return errors.Fatalf("There were %d errors\n", totalErrors)
	}

	if opts.Verify {
		if !gopts.JSON {
			msg.P("verifying files in %s\n", opts.Target)
		}
		var count int
		t0 := time.Now()
		count, err = res.VerifyFiles(ctx, opts.Target)
		if err != nil {
			return err
		}
		if totalErrors > 0 {
			return errors.Fatalf("There were %d errors\n", totalErrors)
		}

		if !gopts.JSON {
			msg.P("finished verifying %d files in %s (took %s)\n", count, opts.Target,
				time.Since(t0).Round(time.Millisecond))
		}
	}

	return nil
}
