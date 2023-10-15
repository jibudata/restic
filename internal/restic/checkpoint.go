package restic

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/restic/restic/internal/debug"
)

const (
	CheckpointStateInProgress = "in-progress"
	CheckpointStateFailed     = "failed"
	CheckpointStateCompleted  = "completed"
)

// Checkpoint is the state of a restored resource at one point in time.
// Checkpoint.Snapshot is a state of saving tree blobs
//
// Normally when last restore complete, the state of restore is same as
// backup snapshot (say sn1), then OriginalSnapshotId = sn1.
// Next time when restore backup snapshot sn2, there are 2 possibilities:
// 1. Restore files has not changed at all
// 2. Restore files changed (e.g., last restore was not completed, or user
// changed some files)
// The Checkpoint.Snapshot is used to do a fast compare to get result of
// 1 or 2, then perform incremental restore based on that result.
type Checkpoint struct {
	Snapshot           `json:",inline"`
	OriginalSnapshotId *ID `json:"originalSnapshotId,omitempty"`
	Parent             *ID `json:"parent,omitempty"`

	// Does not support json update yet
	//State              string `json:"status,omitempty"`

	id *ID // checkpoint plaintext ID
}

func NewCheckpoint(oid *ID) *Checkpoint {
	return &Checkpoint{
		OriginalSnapshotId: oid,
	}
}

// LoadCheckpoint loads the checkpoint with the id and returns it.
func LoadCheckpoint(ctx context.Context, repo Repository, id ID) (*Checkpoint, error) {
	cp := &Checkpoint{id: &id}
	err := repo.LoadJSONUnpacked(ctx, CheckpointFile, id, cp)
	if err != nil {
		return nil, err
	}

	return cp, nil
}

const loadCheckpointParallelism = 5

// ForAllCheckpoints is similar to ForAllSnapshots from snapshot.go
func ForAllCheckpoints(ctx context.Context, repo Repository, excludeIDs IDSet, fn func(ID, *Checkpoint, error) error) error {
	var m sync.Mutex
	// track spawned goroutines using wg, create a new context which is
	// cancelled as soon as an error occurs.
	wg, ctx := errgroup.WithContext(ctx)

	ch := make(chan ID)

	// send list of checkpoint files through ch, which is closed afterwards
	wg.Go(func() error {
		defer close(ch)
		return repo.List(ctx, CheckpointFile, func(id ID, size int64) error {
			if excludeIDs.Has(id) {
				return nil
			}

			select {
			case <-ctx.Done():
				return nil
			case ch <- id:
			}
			return nil
		})
	})

	// a worker receives an checkpoint ID from ch, loads the checkpoint
	// and runs fn with id, the checkpoint and the error
	worker := func() error {
		for id := range ch {
			debug.Log("load checkpoint %v", id)
			sn, err := LoadCheckpoint(ctx, repo, id)

			m.Lock()
			err = fn(id, sn, err)
			m.Unlock()
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := 0; i < loadCheckpointParallelism; i++ {
		wg.Go(worker)
	}

	return wg.Wait()
}

func (sn Checkpoint) String() string {
	return fmt.Sprintf("<Checkpoint %s of %v at %s by %s@%s>",
		sn.id.Str(), sn.Paths, sn.Time, sn.Username, sn.Hostname)
}

// ID returns the checkpoint's ID.
func (sn Checkpoint) ID() *ID {
	return sn.id
}

//func (sn *Checkpoint) fillUserInfo() error {
//	usr, err := user.Current()
//	if err != nil {
//		return nil
//	}
//	sn.Username = usr.Username
//
//	// set userid and groupid
//	sn.UID, sn.GID, err = uidGidInt(*usr)
//	return err
//}

// AddTags adds the given tags to the checkpoint tags, preventing duplicates.
// It returns true if any changes were made.
func (sn *Checkpoint) AddTags(addTags []string) (changed bool) {
nextTag:
	for _, add := range addTags {
		for _, tag := range sn.Tags {
			if tag == add {
				continue nextTag
			}
		}
		sn.Tags = append(sn.Tags, add)
		changed = true
	}
	return
}

// RemoveTags removes the given tags from the checkpoint tags and
// returns true if any changes were made.
func (sn *Checkpoint) RemoveTags(removeTags []string) (changed bool) {
	for _, remove := range removeTags {
		for i, tag := range sn.Tags {
			if tag == remove {
				// https://github.com/golang/go/wiki/SliceTricks
				sn.Tags[i] = sn.Tags[len(sn.Tags)-1]
				sn.Tags[len(sn.Tags)-1] = ""
				sn.Tags = sn.Tags[:len(sn.Tags)-1]

				changed = true
				break
			}
		}
	}
	return
}

func (sn *Checkpoint) hasTag(tag string) bool {
	for _, snTag := range sn.Tags {
		if tag == snTag {
			return true
		}
	}
	return false
}

// HasTags returns true if the checkpoint has all the tags in l.
func (sn *Checkpoint) HasTags(l []string) bool {
	for _, tag := range l {
		if tag == "" && len(sn.Tags) == 0 {
			return true
		}
		if !sn.hasTag(tag) {
			return false
		}
	}

	return true
}

// HasTagList returns true if either
//   - the checkpoint satisfies at least one TagList, so there is a TagList in l
//     for which all tags are included in sn, or
//   - l is empty
func (sn *Checkpoint) HasTagList(l []TagList) bool {
	debug.Log("testing checkpoint with tags %v against list: %v", sn.Tags, l)

	if len(l) == 0 {
		return true
	}

	for _, tags := range l {
		if sn.HasTags(tags) {
			debug.Log("  checkpoint satisfies %v %v", tags, l)
			return true
		}
	}

	return false
}

func (sn *Checkpoint) hasPath(path string) bool {
	for _, snPath := range sn.Paths {
		if path == snPath {
			return true
		}
	}
	return false
}

// HasPaths returns true if the checkpoint has all of the paths.
func (sn *Checkpoint) HasPaths(paths []string) bool {
	for _, path := range paths {
		if !sn.hasPath(path) {
			return false
		}
	}

	return true
}

// HasHostname returns true if either
// - the checkpoint hostname is in the list of the given hostnames, or
// - the list of given hostnames is empty
func (sn *Checkpoint) HasHostname(hostnames []string) bool {
	if len(hostnames) == 0 {
		return true
	}

	for _, hostname := range hostnames {
		if sn.Hostname == hostname {
			return true
		}
	}

	return false
}

// Checkpoints is a list of checkpoint.
type Checkpoints []*Checkpoint

// Len returns the number of checkpoint in sn.
func (sn Checkpoints) Len() int {
	return len(sn)
}

// Less returns true iff the ith checkpoint has been made after the jth.
func (sn Checkpoints) Less(i, j int) bool {
	return sn[i].Time.After(sn[j].Time)
}

// Swap exchanges the two checkpoint.
func (sn Checkpoints) Swap(i, j int) {
	sn[i], sn[j] = sn[j], sn[i]
}
