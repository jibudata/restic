package restic

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/restic/restic/internal/errors"
)

var ErrNoCheckpointFound = errors.New("no checkpoint found")

// FindLatestCheckpoint finds latest checkpoint with optional target/directory, tags, hostname, and timestamp filters.
func FindLatestCheckpoint(ctx context.Context, repo Repository, targets []string, tagLists []TagList, hostnames []string, timeStampLimit *time.Time) (ID, error) {
	var err error
	absTargets := make([]string, 0, len(targets))
	for _, target := range targets {
		if !filepath.IsAbs(target) {
			target, err = filepath.Abs(target)
			if err != nil {
				return ID{}, errors.Wrap(err, "Abs")
			}
		}
		absTargets = append(absTargets, filepath.Clean(target))
	}

	var (
		latest   time.Time
		latestID ID
		found    bool
	)

	err = ForAllCheckpoints(ctx, repo, nil, func(id ID, checkpoint *Checkpoint, err error) error {
		if err != nil {
			return errors.Errorf("Error loading checkpoint %v: %v", id.Str(), err)
		}

		if timeStampLimit != nil && checkpoint.Time.After(*timeStampLimit) {
			return nil
		}

		if checkpoint.Time.Before(latest) {
			return nil
		}

		if !checkpoint.HasHostname(hostnames) {
			return nil
		}

		if !checkpoint.HasTagList(tagLists) {
			return nil
		}

		if !checkpoint.HasPaths(absTargets) {
			return nil
		}

		latest = checkpoint.Time
		latestID = id
		found = true
		return nil
	})

	if err != nil {
		return ID{}, err
	}

	if !found {
		return ID{}, ErrNoCheckpointFound
	}

	return latestID, nil
}

// FindCheckpoint takes a string and tries to find a checkpoint whose ID matches
// the string as closely as possible.
func FindCheckpoint(ctx context.Context, repo Repository, s string) (ID, error) {
	// find snapshot id with prefix
	name, err := Find(ctx, repo.Backend(), CheckpointFile, s)
	if err != nil {
		return ID{}, err
	}

	return ParseID(name)
}

// Implement FindFilteredCheckpoints function which is similar to FindFilteredSnapshots
func FindFilteredCheckpoints(ctx context.Context, repo Repository, hosts []string, tags []TagList, paths []string) (Checkpoints, error) {
	results := make(Checkpoints, 0, 20)

	err := ForAllCheckpoints(ctx, repo, nil, func(id ID, cp *Checkpoint, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not load snapshot %v: %v\n", id.Str(), err)
			return nil
		}

		if !cp.HasHostname(hosts) || !cp.HasTagList(tags) || !cp.HasPaths(paths) {
			return nil
		}

		results = append(results, cp)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}
