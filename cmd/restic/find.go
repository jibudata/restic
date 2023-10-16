package main

import (
	"context"

	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

// FindFilteredSnapshots yields Snapshots, either given explicitly by `snapshotIDs` or filtered from the list of all snapshots.
func FindFilteredSnapshots(ctx context.Context, repo *repository.Repository, hosts []string, tags []restic.TagList, paths []string, snapshotIDs []string) <-chan *restic.Snapshot {
	out := make(chan *restic.Snapshot)
	go func() {
		defer close(out)
		if len(snapshotIDs) != 0 {
			var (
				id         restic.ID
				usedFilter bool
				err        error
			)
			ids := make(restic.IDs, 0, len(snapshotIDs))
			// Process all snapshot IDs given as arguments.
			for _, s := range snapshotIDs {
				if s == "latest" {
					usedFilter = true
					id, err = restic.FindLatestSnapshot(ctx, repo, paths, tags, hosts, nil)
					if err != nil {
						Warnf("Ignoring %q, no snapshot matched given filter (Paths:%v Tags:%v Hosts:%v)\n", s, paths, tags, hosts)
						continue
					}
				} else {
					id, err = restic.FindSnapshot(ctx, repo, s)
					if err != nil {
						Warnf("Ignoring %q: %v\n", s, err)
						continue
					}
				}
				ids = append(ids, id)
			}

			// Give the user some indication their filters are not used.
			if !usedFilter && (len(hosts) != 0 || len(tags) != 0 || len(paths) != 0) {
				Warnf("Ignoring filters as there are explicit snapshot ids given\n")
			}

			for _, id := range ids.Uniq() {
				sn, err := restic.LoadSnapshot(ctx, repo, id)
				if err != nil {
					Warnf("Ignoring %q, could not load snapshot: %v\n", id, err)
					continue
				}
				select {
				case <-ctx.Done():
					return
				case out <- sn:
				}
			}
			return
		}

		snapshots, err := restic.FindFilteredSnapshots(ctx, repo, hosts, tags, paths)
		if err != nil {
			Warnf("could not load snapshots: %v\n", err)
			return
		}

		for _, sn := range snapshots {
			select {
			case <-ctx.Done():
				return
			case out <- sn:
			}
		}
	}()
	return out
}

// FindFilteredCheckpoints yields Checkpoints, either given explicitly by `checkpointIDs` or filtered from the list of all checkpoints.
func FindFilteredCheckpoints(ctx context.Context, repo *repository.Repository, hosts []string, tags []restic.TagList, paths []string, checkpointIDs []string) <-chan *restic.Checkpoint {
	out := make(chan *restic.Checkpoint)
	go func() {
		defer close(out)
		if len(checkpointIDs) != 0 {
			var (
				id         restic.ID
				usedFilter bool
				err        error
			)
			ids := make(restic.IDs, 0, len(checkpointIDs))
			// Process all checkpoint IDs given as arguments.
			for _, s := range checkpointIDs {
				if s == "latest" {
					usedFilter = true
					id, err = restic.FindLatestCheckpoint(ctx, repo, paths, tags, hosts, nil)
					if err != nil {
						Warnf("Ignoring %q, no checkpoint matched given filter (Paths:%v Tags:%v Hosts:%v)\n", s, paths, tags, hosts)
						continue
					}
				} else {
					id, err = restic.FindCheckpoint(ctx, repo, s)
					if err != nil {
						Warnf("Ignoring %q: %v\n", s, err)
						continue
					}
				}
				ids = append(ids, id)
			}

			// Give the user some indication their filters are not used.
			if !usedFilter && (len(hosts) != 0 || len(tags) != 0 || len(paths) != 0) {
				Warnf("Ignoring filters as there are explicit checkpoint ids given\n")
			}

			for _, id := range ids.Uniq() {
				sn, err := restic.LoadCheckpoint(ctx, repo, id)
				if err != nil {
					Warnf("Ignoring %q, could not load checkpoint: %v\n", id, err)
					continue
				}
				select {
				case <-ctx.Done():
					return
				case out <- sn:
				}
			}
			return
		}

		checkpoints, err := restic.FindFilteredCheckpoints(ctx, repo, hosts, tags, paths)
		if err != nil {
			Warnf("could not load checkpoints: %v\n", err)
			return
		}

		for _, sn := range checkpoints {
			select {
			case <-ctx.Done():
				return
			case out <- sn:
			}
		}
	}()
	return out
}
