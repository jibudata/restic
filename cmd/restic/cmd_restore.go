package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/filter"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/restorer"
	"github.com/restic/restic/internal/ui/backup"
	"github.com/restic/restic/internal/ui/termstatus"

	"github.com/spf13/cobra"
	tomb "gopkg.in/tomb.v2"
)

var cmdRestore = &cobra.Command{
	Use:   "restore [flags] snapshotID",
	Short: "Extract the data from a snapshot",
	Long: `
The "restore" command extracts the data from a snapshot from the repository to
a directory.

The special snapshot "latest" can be used to restore the latest snapshot in the
repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRestore(restoreOptions, globalOptions, args)
	},
}

// RestoreOptions collects all options for the restore command.
type RestoreOptions struct {
	Exclude            []string
	InsensitiveExclude []string
	Include            []string
	InsensitiveInclude []string
	Target             string
	Hosts              []string
	Paths              []string
	Tags               restic.TagLists
	Verify             bool
}

var restoreOptions RestoreOptions

// ErrInvalidCheckpointData is used to report an incomplete restore
var ErrInvalidCheckpointData = errors.New("restore checkpoint has problems")

func init() {
	cmdRoot.AddCommand(cmdRestore)

	flags := cmdRestore.Flags()
	flags.StringArrayVarP(&restoreOptions.Exclude, "exclude", "e", nil, "exclude a `pattern` (can be specified multiple times)")
	flags.StringArrayVar(&restoreOptions.InsensitiveExclude, "iexclude", nil, "same as `--exclude` but ignores the casing of filenames")
	flags.StringArrayVarP(&restoreOptions.Include, "include", "i", nil, "include a `pattern`, exclude everything else (can be specified multiple times)")
	flags.StringArrayVar(&restoreOptions.InsensitiveInclude, "iinclude", nil, "same as `--include` but ignores the casing of filenames")
	flags.StringVarP(&restoreOptions.Target, "target", "t", "", "directory to extract data to")

	flags.StringArrayVarP(&restoreOptions.Hosts, "host", "H", nil, `only consider snapshots for this host when the snapshot ID is "latest" (can be specified multiple times)`)
	flags.Var(&restoreOptions.Tags, "tag", "only consider snapshots which include this `taglist` for snapshot ID \"latest\"")
	flags.StringArrayVar(&restoreOptions.Paths, "path", nil, "only consider snapshots which include this (absolute) `path` for snapshot ID \"latest\"")
	flags.BoolVar(&restoreOptions.Verify, "verify", false, "verify restored files content")
}

func runRestore(opts RestoreOptions, gopts GlobalOptions, args []string) error {
	ctx := gopts.ctx
	hasExcludes := len(opts.Exclude) > 0 || len(opts.InsensitiveExclude) > 0
	hasIncludes := len(opts.Include) > 0 || len(opts.InsensitiveInclude) > 0

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

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	if !gopts.NoLock {
		lock, err := lockRepo(ctx, repo)
		defer unlockRepo(lock)
		if err != nil {
			return err
		}
	}

	err = repo.LoadIndex(ctx)
	if err != nil {
		return err
	}

	var id restic.ID

	if snapshotIDString == "latest" {
		id, err = restic.FindLatestSnapshot(ctx, repo, opts.Paths, opts.Tags, opts.Hosts, nil)
		if err != nil {
			Exitf(1, "latest snapshot for criteria not found: %v Paths:%v Hosts:%v", err, opts.Paths, opts.Hosts)
		}
	} else {
		id, err = restic.FindSnapshot(ctx, repo, snapshotIDString)
		if err != nil {
			Exitf(1, "invalid id %q: %v", snapshotIDString, err)
		}
	}

	res, err := restorer.NewRestorer(ctx, repo, id)
	if err != nil {
		Exitf(2, "creating restorer failed: %v\n", err)
	}

	totalErrors := 0
	res.Error = func(location string, err error) error {
		Warnf("ignoring error for %s: %s\n", location, err)
		totalErrors++
		return nil
	}

	excludePatterns := filter.ParsePatterns(opts.Exclude)
	insensitiveExcludePatterns := filter.ParsePatterns(opts.InsensitiveExclude)
	selectExcludeFilter := func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool) {
		matched, err := filter.List(excludePatterns, item)
		if err != nil {
			Warnf("error for exclude pattern: %v", err)
		}

		matchedInsensitive, err := filter.List(insensitiveExcludePatterns, strings.ToLower(item))
		if err != nil {
			Warnf("error for iexclude pattern: %v", err)
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
			Warnf("error for include pattern: %v", err)
		}

		matchedInsensitive, childMayMatchInsensitive, err := filter.ListWithChild(insensitiveIncludePatterns, strings.ToLower(item))
		if err != nil {
			Warnf("error for iexclude pattern: %v", err)
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

	Verbosef("restoring %s to %s\n", res.Snapshot(), opts.Target)

	err = res.RestoreTo(ctx, opts.Target)
	if err != nil {
		return err
	}

	if totalErrors > 0 {
		return errors.Fatalf("There were %d errors\n", totalErrors)
	}

	if opts.Verify {
		Verbosef("verifying files in %s\n", opts.Target)
		var count int
		t0 := time.Now()
		count, err = res.VerifyFiles(ctx, opts.Target)
		if err != nil {
			return err
		}
		if totalErrors > 0 {
			return errors.Fatalf("There were %d errors\n", totalErrors)
		}
		Verbosef("finished verifying %d files in %s (took %s)\n", count, opts.Target,
			time.Since(t0).Round(time.Millisecond))
	}

	// TBD - determine if restore data has changed since last restore
	err = doCheckpoint(opts, gopts, repo, &id, false)
	if err != nil {
		Verbosef("restore checkpoint failed: %v\n", err)
	}

	return err
}

// collectRejectByNameFuncs returns a list of all functions which may reject data
// from being saved in a snapshot based on path only
func collectRejectByNameFuncsForRestore(opts RestoreOptions, repo *repository.Repository) (fs []RejectByNameFunc, err error) {
	// exclude restic cache
	if repo.Cache != nil {
		f, err := rejectResticCache(repo)
		if err != nil {
			return nil, err
		}

		fs = append(fs, f)
	}

	if len(opts.InsensitiveExclude) > 0 {
		fs = append(fs, rejectByInsensitivePattern(opts.InsensitiveExclude))
	}

	if len(opts.Exclude) > 0 {
		fs = append(fs, rejectByPattern(opts.Exclude))
	}
	return fs, nil
}

// parent returns the ID of the parent checkpoint. If there is none, nil is
// returned.
func findParentCheckpoint(ctx context.Context, repo restic.Repository, opts RestoreOptions, targets []string, timeStampLimit time.Time) (parentID *restic.ID, err error) {
	// Find last snapshot to set it as parent, if not already set
	id, err := restic.FindLatestCheckpoint(ctx, repo, targets, []restic.TagList{}, opts.Hosts, &timeStampLimit)
	if err == nil {
		parentID = &id
	} else if err != restic.ErrNoCheckpointFound {
		return nil, err
	}

	return parentID, nil
}

// Save checkpoint for restore
func doCheckpoint(opts RestoreOptions, gopts GlobalOptions, repo *repository.Repository, snapshotId *restic.ID, checkLastSnapshotSync bool) error {

	var t tomb.Tomb
	var err error

	term := termstatus.New(globalOptions.stdout, globalOptions.stderr, globalOptions.Quiet)
	t.Go(func() error { term.Run(t.Context(globalOptions.ctx)); return nil })

	var progressPrinter backup.ProgressPrinter
	if gopts.JSON {
		progressPrinter = backup.NewJSONProgress(term, gopts.verbosity)
	} else {
		progressPrinter = backup.NewTextProgress(term, gopts.verbosity)
	}
	progressReporter := backup.NewProgress(progressPrinter)

	progressPrinter.V("start restore checkpoint")

	//if opts.DryRun {
	//	repo.SetDryRun()
	//	progressReporter.SetDryRun()
	//}

	// use the terminal for stdout/stderr
	prevStdout, prevStderr := gopts.stdout, gopts.stderr
	defer func() {
		gopts.stdout, gopts.stderr = prevStdout, prevStderr
	}()
	gopts.stdout, gopts.stderr = progressPrinter.Stdout(), progressPrinter.Stderr()

	progressReporter.SetMinUpdatePause(calculateProgressInterval(!gopts.Quiet, gopts.JSON))

	t.Go(func() error { return progressReporter.Run(t.Context(gopts.ctx)) })

	if !gopts.JSON {
		progressPrinter.V("lock repository")
	}

	if gopts.NoLock {
		lock, err := lockRepo(gopts.ctx, repo)
		defer unlockRepo(lock)
		if err != nil {
			return err
		}
	}

	var id restic.ID
	if checkLastSnapshotSync {
		checkPoint := restic.NewCheckpoint(snapshotId)
		// save checkpoint json to repo
		id, err = repo.SaveJSONUnpacked(context.TODO(), restic.CheckpointFile, *checkPoint)
		if err != nil {
			return err
		}

	} else {

		var targetFS fs.FS = fs.Local{}
		var targets []string

		timeStamp := time.Now()

		targets = append(targets, opts.Target)

		// rejectByNameFuncs collect functions that can reject items from the backup based on path only
		rejectByNameFuncs, err := collectRejectByNameFuncsForRestore(opts, repo)
		if err != nil {
			return err
		}

		selectByNameFilter := func(item string) bool {
			for _, reject := range rejectByNameFuncs {
				if reject(item) {
					return false
				}
			}
			return true
		}

		sc := archiver.NewScanner(targetFS)
		sc.SelectByName = selectByNameFilter
		//sc.Select = selectFilter
		sc.Error = progressReporter.ScannerError
		sc.Result = progressReporter.ReportTotal

		if !gopts.JSON {
			progressPrinter.V("start scan on %v", opts.Target)
		}
		t.Go(func() error { return sc.Scan(t.Context(gopts.ctx), targets) })

		arch := archiver.New(repo, targetFS, archiver.Options{})
		arch.SelectByName = selectByNameFilter
		//arch.Select = selectFilter
		//arch.WithAtime = opts.WithAtime
		success := true
		arch.Error = func(item string, fi os.FileInfo, err error) error {
			success = false
			return progressReporter.Error(item, fi, err)
		}
		arch.CompleteItem = progressReporter.CompleteItem
		arch.StartFile = progressReporter.StartFile
		arch.CompleteBlob = progressReporter.CompleteBlob

		var parentCheckpointID *restic.ID
		parentCheckpointID, err = findParentCheckpoint(gopts.ctx, repo, opts, targets, timeStamp)
		if err != nil {
			return err
		}

		if !gopts.JSON {
			if parentCheckpointID != nil {
				progressPrinter.P("using parent checkpoint %v\n", parentCheckpointID.Str())
			} else {
				progressPrinter.P("no parent checkpoint found, will read all files\n")
			}
		}

		var originalSnapshotId *restic.ID
		if parentCheckpointID == nil {
			parentCheckpointID = &restic.ID{}
			originalSnapshotId = snapshotId
		} else {
			cp, err := restic.LoadCheckpoint(gopts.ctx, repo, *parentCheckpointID)
			if err != nil {
				debug.Log("unable to load parent checkpoint %v: %v", parentCheckpointID, err)
				return nil
			}
			originalSnapshotId = cp.OriginalSnapshotId
		}

		var hostName string
		if len(opts.Hosts) > 0 {
			hostName = opts.Hosts[0]
		}

		snapshotOpts := archiver.SnapshotOptions{
			Excludes:         opts.Exclude,
			Tags:             opts.Tags.Flatten(),
			Time:             timeStamp,
			Hostname:         hostName,
			ParentSnapshot:   *originalSnapshotId,
			ParentCheckpoint: *parentCheckpointID,
		}

		if !gopts.JSON {
			progressPrinter.V("start backup on %v", targets)
		}
		_, id, err = arch.Checkpoint(gopts.ctx, targets, snapshotOpts)

		// cleanly shutdown all running goroutines
		t.Kill(nil)

		werr := t.Wait()

		// return original error
		if err != nil {
			return errors.Fatalf("unable to save checkpoint: %v", err)
		}

		// Report finished execution
		progressReporter.Finish(id)
		if !gopts.JSON {
			progressPrinter.P("checkpoint %s saved\n", id.Str())
		}
		if !success {
			return ErrInvalidCheckpointData
		}

		return werr
	}

	return nil
}
