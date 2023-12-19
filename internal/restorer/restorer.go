package restorer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"

	"github.com/restic/chunker"
	internalchunker "github.com/restic/restic/internal/chunker"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	restoreui "github.com/restic/restic/internal/ui/restore"
	"golang.org/x/sync/errgroup"
)

// Restorer is used to restore a snapshot to a directory.
type Restorer struct {
	repo             restic.Repository
	sn               *restic.Snapshot
	sparse           bool
	skipExisting     bool
	reChunk          bool
	quickChangeCheck bool
	fileChunkInfoMap *internalchunker.FileChunkInfoMap

	progress *restoreui.Progress

	Error        func(location string, err error) error
	SelectFilter func(item string, dstpath string, node *restic.Node) (selectedForRestore bool, childMayBeSelected bool)
}

var restorerAbortOnAllErrors = func(location string, err error) error { return err }

type Option func(*Restorer)

func WithSkipExisting(skipExisting bool) Option {
	return func(r *Restorer) {
		r.skipExisting = skipExisting
	}
}

func WithReChunk(reChunk bool) Option {
	return func(r *Restorer) {
		r.reChunk = reChunk
	}
}

func WithQuickChangeCheck(quickChangeCheck bool) Option {
	return func(r *Restorer) {
		r.quickChangeCheck = quickChangeCheck
	}
}

func WithFileChunkInfoMap(fileChunkInfoMap *internalchunker.FileChunkInfoMap) Option {
	return func(r *Restorer) {
		r.fileChunkInfoMap = fileChunkInfoMap
	}
}

// NewRestorer creates a restorer preloaded with the content from the snapshot id.
func NewRestorer(repo restic.Repository, sn *restic.Snapshot, sparse bool,
	progress *restoreui.Progress, options ...Option) *Restorer {

	r := &Restorer{
		repo:         repo,
		sparse:       sparse,
		Error:        restorerAbortOnAllErrors,
		SelectFilter: func(string, string, *restic.Node) (bool, bool) { return true, true },
		progress:     progress,
		sn:           sn,
	}

	for _, option := range options {
		option(r)
	}

	yes, err := strconv.ParseBool(os.Getenv("RESTIC_RECHUNK"))
	if err == nil {
		r.reChunk = yes
	}

	yes, err = strconv.ParseBool(os.Getenv("RESTIC_QUICK_CHANGE_CHECK"))
	if err == nil {
		r.quickChangeCheck = yes
	}

	yes, err = strconv.ParseBool(os.Getenv("RESTIC_SKIP_EXISTING"))
	if err == nil {
		r.skipExisting = yes
	}

	return r
}

type treeVisitor struct {
	enterDir  func(node *restic.Node, target, location string) error
	visitNode func(node *restic.Node, target, location string) error
	leaveDir  func(node *restic.Node, target, location string) error
}

// traverseTree traverses a tree from the repo and calls treeVisitor.
// target is the path in the file system, location within the snapshot.
func (res *Restorer) traverseTree(ctx context.Context, target, location string, treeID restic.ID, visitor treeVisitor) (hasRestored bool, err error) {
	debug.Log("%v %v %v", target, location, treeID)
	tree, err := restic.LoadTree(ctx, res.repo, treeID)
	if err != nil {
		debug.Log("error loading tree %v: %v", treeID, err)
		return hasRestored, res.Error(location, err)
	}

	for _, node := range tree.Nodes {

		// ensure that the node name does not contain anything that refers to a
		// top-level directory.
		nodeName := filepath.Base(filepath.Join(string(filepath.Separator), node.Name))
		if nodeName != node.Name {
			debug.Log("node %q has invalid name %q", node.Name, nodeName)
			err := res.Error(location, errors.Errorf("invalid child node name %s", node.Name))
			if err != nil {
				return hasRestored, err
			}
			continue
		}

		nodeTarget := filepath.Join(target, nodeName)
		nodeLocation := filepath.Join(location, nodeName)

		if target == nodeTarget || !fs.HasPathPrefix(target, nodeTarget) {
			debug.Log("target: %v %v", target, nodeTarget)
			debug.Log("node %q has invalid target path %q", node.Name, nodeTarget)
			err := res.Error(nodeLocation, errors.New("node has invalid path"))
			if err != nil {
				return hasRestored, err
			}
			continue
		}

		// sockets cannot be restored
		if node.Type == "socket" {
			continue
		}

		selectedForRestore, childMayBeSelected := res.SelectFilter(nodeLocation, nodeTarget, node)
		debug.Log("SelectFilter returned %v %v for %q", selectedForRestore, childMayBeSelected, nodeLocation)

		if selectedForRestore {
			hasRestored = true
		}

		sanitizeError := func(err error) error {
			switch err {
			case nil, context.Canceled, context.DeadlineExceeded:
				// Context errors are permanent.
				return err
			default:
				return res.Error(nodeLocation, err)
			}
		}

		if node.Type == "dir" {
			if node.Subtree == nil {
				return hasRestored, errors.Errorf("Dir without subtree in tree %v", treeID.Str())
			}

			if selectedForRestore && visitor.enterDir != nil {
				err = sanitizeError(visitor.enterDir(node, nodeTarget, nodeLocation))
				if err != nil {
					return hasRestored, err
				}
			}

			// keep track of restored child status
			// so metadata of the current directory are restored on leaveDir
			childHasRestored := false

			if childMayBeSelected {
				childHasRestored, err = res.traverseTree(ctx, nodeTarget, nodeLocation, *node.Subtree, visitor)
				err = sanitizeError(err)
				if err != nil {
					return hasRestored, err
				}
				// inform the parent directory to restore parent metadata on leaveDir if needed
				if childHasRestored {
					hasRestored = true
				}
			}

			// metadata need to be restore when leaving the directory in both cases
			// selected for restore or any child of any subtree have been restored
			if (selectedForRestore || childHasRestored) && visitor.leaveDir != nil {
				err = sanitizeError(visitor.leaveDir(node, nodeTarget, nodeLocation))
				if err != nil {
					return hasRestored, err
				}
			}

			continue
		}

		if selectedForRestore {
			err = sanitizeError(visitor.visitNode(node, nodeTarget, nodeLocation))
			if err != nil {
				return hasRestored, err
			}
		}
	}

	return hasRestored, nil
}

func (res *Restorer) restoreNodeTo(ctx context.Context, node *restic.Node, target, location string) error {
	debug.Log("restoreNode %v %v %v", node.Name, target, location)

	err := node.CreateAt(ctx, target, res.repo)
	if err != nil {
		debug.Log("node.CreateAt(%s) error %v", target, err)
		return err
	}

	if res.progress != nil {
		res.progress.AddProgress(location, 0, 0)
	}

	return res.restoreNodeMetadataTo(node, target, location)
}

func (res *Restorer) restoreNodeMetadataTo(node *restic.Node, target, location string) error {
	debug.Log("restoreNodeMetadata %v %v %v", node.Name, target, location)
	err := node.RestoreMetadata(target)
	if err != nil {
		debug.Log("node.RestoreMetadata(%s) error %v", target, err)
	}
	return err
}

func (res *Restorer) restoreHardlinkAt(node *restic.Node, target, path, location string) error {
	if err := fs.Remove(path); !os.IsNotExist(err) {
		return errors.Wrap(err, "RemoveCreateHardlink")
	}
	err := fs.Link(target, path)
	if err != nil {
		return errors.WithStack(err)
	}

	if res.progress != nil {
		res.progress.AddProgress(location, 0, 0)
	}

	// TODO investigate if hardlinks have separate metadata on any supported system
	return res.restoreNodeMetadataTo(node, path, location)
}

func (res *Restorer) restoreEmptyFileAt(node *restic.Node, target, location string) error {
	wr, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = wr.Close()
	if err != nil {
		return err
	}

	if res.progress != nil {
		res.progress.AddProgress(location, 0, 0)
	}

	return res.restoreNodeMetadataTo(node, target, location)
}

// RestoreTo creates the directories and files in the snapshot below dst.
// Before an item is created, res.Filter is called.
func (res *Restorer) RestoreTo(ctx context.Context, dst string) error {
	var err error
	if !filepath.IsAbs(dst) {
		dst, err = filepath.Abs(dst)
		if err != nil {
			return errors.Wrap(err, "Abs")
		}
	}

	currentFiles := make(map[string]existingFileInfo)
	err = filepath.Walk(dst, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if path == dst {
			return nil
		}
		currentFiles[path] = existingFileInfo{location: path, info: info}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "Failed to get current files")
	}

	idx := NewHardlinkIndex()
	filerestorer := newFileRestorer(dst, res.repo.Backend().Load, res.repo.Key(), res.repo.Index().Lookup,
		res.repo.Connections(), res.sparse, res.progress)
	filerestorer.Error = res.Error

	debug.Log("first pass for %q", dst)

	// first tree pass: create directories and collect all files to restore
	_, err = res.traverseTree(ctx, dst, string(filepath.Separator), *res.sn.Tree, treeVisitor{
		enterDir: func(node *restic.Node, target, location string) error {
			debug.Log("first pass, enterDir: mkdir %q, leaveDir should restore metadata", location)
			if res.progress != nil {
				res.progress.AddFile(0)
			}

			defer delete(currentFiles, target)

			// create dir with default permissions
			// #leaveDir restores dir metadata after visiting all children
			return fs.MkdirAll(target, 0700)
		},

		visitNode: func(node *restic.Node, target, location string) error {
			debug.Log("first pass, visitNode: mkdir %q, leaveDir on second pass should restore metadata", location)

			defer delete(currentFiles, target)

			// create parent dir with default permissions
			// second pass #leaveDir restores dir metadata after visiting/restoring all children
			err := fs.MkdirAll(filepath.Dir(target), 0700)
			if err != nil {
				return err
			}

			if node.Type != "file" {
				if res.progress != nil {
					res.progress.AddFile(0)
				}
				return nil
			}

			if node.Size == 0 {
				if res.progress != nil {
					res.progress.AddFile(node.Size)
				}
				return nil // deal with empty files later
			}

			if node.Links > 1 {
				if idx.Has(node.Inode, node.DeviceID) {
					if res.progress != nil {
						// a hardlinked file does not increase the restore size
						res.progress.AddFile(0)
					}
					return nil
				}
				idx.Add(node.Inode, node.DeviceID, location)
			}

			if res.progress != nil {
				res.progress.AddFile(node.Size)
			}

			currentFile, ok := currentFiles[target]
			if !ok || res.sparse || !res.skipExisting {
				fmt.Printf("DEBUG: Adding new file, location: %s, target: %s\n", location, target)
				filerestorer.addNewFile(location, node.Content, int64(node.Size))
			} else if fileChanged(currentFile.info, node, res.quickChangeCheck) {
				existingBlobs, currentSize, equalFile, err := res.preprocessFile(target, node)
				if err != nil {
					fmt.Printf("DEBUG: Failed to process file %s, err: %s\n", target, err)
					return err
				}

				if !equalFile {
					fmt.Printf("DEBUG: Adding modified file with existing blobs %v, location: %s, target: %s\n", existingBlobs, location, target)
					filerestorer.addModifiedFilesFile(location, node.Content, int64(node.Size), currentSize, existingBlobs)
				} else {
					fmt.Printf("DEBUG: Found unchanged file after comparing, skip restoring, location: %s, target: %s\n", location, target)
				}
			} else {
				fmt.Printf("DEBUG: Found unchanged file, skip restoring, location: %s, target: %s\n", location, target)
			}

			return nil
		},
	})
	if err != nil {
		return err
	}

	for target := range currentFiles {
		location, err := filepath.Rel(dst, target)
		if err != nil {
			fmt.Printf("DEBUG: Failed to find relative path, :%s\n", err)
			return err
		}
		fmt.Printf("DEBUG: Adding stale file, location: %s, target: %s\n", location, target)
		filerestorer.addStaleFilesFile(location)
	}

	err = filerestorer.restoreFiles(ctx)
	if err != nil {
		return err
	}

	debug.Log("second pass for %q", dst)

	// second tree pass: restore special files and filesystem metadata
	_, err = res.traverseTree(ctx, dst, string(filepath.Separator), *res.sn.Tree, treeVisitor{
		visitNode: func(node *restic.Node, target, location string) error {
			debug.Log("second pass, visitNode: restore node %q", location)
			if node.Type != "file" {
				return res.restoreNodeTo(ctx, node, target, location)
			}

			// create empty files, but not hardlinks to empty files
			if node.Size == 0 && (node.Links < 2 || !idx.Has(node.Inode, node.DeviceID)) {
				if node.Links > 1 {
					idx.Add(node.Inode, node.DeviceID, location)
				}
				return res.restoreEmptyFileAt(node, target, location)
			}

			if idx.Has(node.Inode, node.DeviceID) && idx.GetFilename(node.Inode, node.DeviceID) != location {
				return res.restoreHardlinkAt(node, filerestorer.targetPath(idx.GetFilename(node.Inode, node.DeviceID)), target, location)
			}

			return res.restoreNodeMetadataTo(node, target, location)
		},
		leaveDir: func(node *restic.Node, target, location string) error {
			err := res.restoreNodeMetadataTo(node, target, location)
			if err == nil && res.progress != nil {
				res.progress.AddProgress(location, 0, 0)
			}
			return err
		},
	})
	return err
}

// Snapshot returns the snapshot this restorer is configured to use.
func (res *Restorer) Snapshot() *restic.Snapshot {
	return res.sn
}

// Number of workers in VerifyFiles.
const nVerifyWorkers = 8

// VerifyFiles checks whether all regular files in the snapshot res.sn
// have been successfully written to dst. It stops when it encounters an
// error. It returns that error and the number of files it has successfully
// verified.
func (res *Restorer) VerifyFiles(ctx context.Context, dst string) (int, error) {
	type mustCheck struct {
		node *restic.Node
		path string
	}

	var (
		nchecked uint64
		work     = make(chan mustCheck, 2*nVerifyWorkers)
	)

	g, ctx := errgroup.WithContext(ctx)

	// Traverse tree and send jobs to work.
	g.Go(func() error {
		defer close(work)

		_, err := res.traverseTree(ctx, dst, string(filepath.Separator), *res.sn.Tree, treeVisitor{
			visitNode: func(node *restic.Node, target, location string) error {
				if node.Type != "file" {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case work <- mustCheck{node, target}:
					return nil
				}
			},
		})
		return err
	})

	for i := 0; i < nVerifyWorkers; i++ {
		g.Go(func() (err error) {
			var buf []byte
			for job := range work {
				buf, err = res.verifyFile(job.path, job.node, buf)
				if err != nil {
					err = res.Error(job.path, err)
				}
				if err != nil || ctx.Err() != nil {
					break
				}
				atomic.AddUint64(&nchecked, 1)
			}
			return err
		})
	}

	return int(nchecked), g.Wait()
}

// Verify that the file target has the contents of node.
//
// buf and the first return value are scratch space, passed around for reuse.
// Reusing buffers prevents the verifier goroutines allocating all of RAM and
// flushing the filesystem cache (at least on Linux).
func (res *Restorer) verifyFile(target string, node *restic.Node, buf []byte) ([]byte, error) {
	f, err := os.Open(target)
	if err != nil {
		return buf, err
	}
	defer func() {
		_ = f.Close()
	}()

	fi, err := f.Stat()
	switch {
	case err != nil:
		return buf, err
	case int64(node.Size) != fi.Size():
		return buf, errors.Errorf("Invalid file size for %s: expected %d, got %d",
			target, node.Size, fi.Size())
	}

	var offset int64
	for _, blobID := range node.Content {
		length, found := res.repo.LookupBlobSize(blobID, restic.DataBlob)
		if !found {
			return buf, errors.Errorf("Unable to fetch blob %s", blobID)
		}

		if length > uint(cap(buf)) {
			buf = make([]byte, 2*length)
		}
		buf = buf[:length]

		_, err = f.ReadAt(buf, offset)
		if err != nil {
			return buf, err
		}
		if !blobID.Equal(restic.Hash(buf)) {
			return buf, errors.Errorf(
				"Unexpected content in %s, starting at offset %d",
				target, offset)
		}
		offset += int64(length)
	}

	return buf, nil
}

// preprocessFile compares the current file blob hash between the one in node one by one, and returns the equal ones.
// It also returns a bool value to indicate if the current file is equal to the file in snapshot.
// If --chunk-file is specified, it uses the blob hashes from the chunk file instead of calculating them from the file.
// If --rechunk is specified, it will re-chunk the file and compare the blob hashes.
// Otherwise, the chunk sizes from the snapshot files will be used to divide the files on disk for comparison.
// Note:
// If a file contains all the contents of the snapshot, with just some additional content at the end,
// it will still be considered equal, the extra content will be truncated.
func (res *Restorer) preprocessFile(target string, node *restic.Node) (map[int64]struct{}, int64, bool, error) {
	// skip small files
	if node.Size < 2*chunker.MinSize {
		return nil, 0, false, nil
	}

	if res.fileChunkInfoMap != nil {
		return res.preprocessFileByLocalChunkFile(target, node)
	}

	if res.reChunk {
		return res.preprocessFileByChunk(target, node)
	}

	f, err := os.Open(target)
	if err != nil {
		return nil, 0, false, err
	}
	defer func() {
		_ = f.Close()
	}()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, false, err
	}

	existingBlobs := make(map[int64]struct{})

	var offset int64
	var buf []byte
	for _, blobID := range node.Content {
		length, found := res.repo.LookupBlobSize(blobID, restic.DataBlob)
		if !found {
			return nil, 0, false, errors.Errorf("Unable to fetch blob %s", blobID)
		}

		if length > uint(cap(buf)) {
			buf = make([]byte, 2*length)
		}
		buf = buf[:length]

		_, err = f.ReadAt(buf, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, false, err
		}
		if blobID.Equal(restic.Hash(buf)) {
			existingBlobs[offset] = struct{}{}
		}
		offset += int64(length)
	}

	equal := len(existingBlobs) == len(node.Content)
	currentSize := fi.Size()
	if equal {
		// equal files with be skipped in restore, so if file size > snapshot file size, truncate it in advance.
		if currentSize > int64(node.Size) {
			// close the file first as it is opened in read mode
			_ = f.Close()
			err = os.Truncate(target, int64(node.Size))
			if err != nil {
				return nil, 0, false, err
			}
		}
	}

	return existingBlobs, currentSize, equal, nil
}

func (res *Restorer) preprocessFileByChunk(target string, node *restic.Node) (map[int64]struct{}, int64, bool, error) {
	f, err := os.Open(target)
	if err != nil {
		return nil, 0, false, err
	}
	defer func() {
		_ = f.Close()
	}()

	// TODO: store the offset mapping in Node
	var offset int64
	offsetsByBlobID := make(map[restic.ID][]int64)
	for _, blobID := range node.Content {
		length, found := res.repo.LookupBlobSize(blobID, restic.DataBlob)
		if !found {
			return nil, 0, false, errors.Errorf("Unable to fetch blob %s", blobID)
		}

		offsetsByBlobID[blobID] = append(offsetsByBlobID[blobID], offset)
		offset += int64(length)
	}

	tmpFile, err := os.CreateTemp("", "restic-")
	if err != nil {
		return nil, 0, false, err
	}

	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	currentSize := int64(node.Size)

	err = fs.PreallocateFile(tmpFile, currentSize)
	if err != nil {
		// Just log the preallocate error but don't let it cause the restore process to fail.
		// Preallocate might return an error if the filesystem (implementation) does not
		// support preallocation or our parameters combination to the preallocate call
		// This should yield a syscall.ENOTSUP error, but some other errors might also
		// show up.
		debug.Log("Failed to preallocate %v with size %v: %v", tmpFile.Name(), currentSize, err)
	}

	existingBlobs := make(map[int64]struct{})

	pol := res.repo.Config().ChunkerPolynomial
	ck := chunker.New(f, pol)
	buf := make([]byte, chunker.MaxSize)
	for {
		chunk, err := ck.Next(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, false, err
		}

		offsets, ok := offsetsByBlobID[restic.Hash(chunk.Data)]
		if ok {
			for _, offset1 := range offsets {
				existingBlobs[offset1] = struct{}{}
				_, err = tmpFile.WriteAt(chunk.Data, offset1)
				if err != nil {
					return nil, 0, false, err
				}
			}
		}
	}

	err = tmpFile.Close()
	if err != nil {
		return nil, 0, false, err
	}

	err = f.Close()
	if err != nil {
		return nil, 0, false, err
	}

	// move tmpFile to target
	err = os.Rename(tmpFile.Name(), target)
	if err != nil {
		return nil, 0, false, err
	}

	return existingBlobs, currentSize, len(existingBlobs) == len(node.Content), nil
}

func (res *Restorer) preprocessFileByLocalChunkFile(target string, node *restic.Node) (map[int64]struct{}, int64, bool, error) {
	fileChunkInfos := *res.fileChunkInfoMap
	fileChunks := fileChunkInfos[target]
	if fileChunks == nil {
		return nil, 0, false, nil
	}

	var offset int64
	existingOffsetsByBlobID := make(map[restic.ID][]int64)
	existingBlobs := make(map[int64]struct{})
	var snapshotOffsets []int64
	for _, blobID := range node.Content {
		length, found := res.repo.LookupBlobSize(blobID, restic.DataBlob)
		if !found {
			return nil, 0, false, errors.Errorf("Unable to fetch blob %s", blobID)
		}

		id := blobID.String()
		_, ok := fileChunks.Chunks[id]
		if ok {
			existingOffsetsByBlobID[blobID] = append(existingOffsetsByBlobID[blobID], offset)
			existingBlobs[offset] = struct{}{}
		}

		snapshotOffsets = append(snapshotOffsets, offset)
		offset += int64(length)
	}

	if len(existingBlobs) == 0 {
		return nil, 0, false, nil
	}

	currentSize := fileChunks.Size

	// if offsets are equal, we can reuse the blobs in the current file directly
	if offsetsEqual(fileChunks.OffSets, snapshotOffsets) {
		equal := len(existingBlobs) == len(node.Content)
		if equal {
			// equal files with be skipped in restore, so if file size > snapshot file size, truncate it in advance.
			if currentSize > int64(node.Size) {
				err := os.Truncate(target, int64(node.Size))
				if err != nil {
					return nil, 0, false, err
				}
			}
		}

		return existingBlobs, currentSize, equal, nil
	}

	f, err := os.Open(target)
	if err != nil {
		return nil, 0, false, err
	}
	defer func() {
		_ = f.Close()
	}()

	tmpFile, err := os.CreateTemp("", "restic-")
	if err != nil {
		return nil, 0, false, err
	}

	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	err = fs.PreallocateFile(tmpFile, currentSize)
	if err != nil {
		// Just log the preallocate error but don't let it cause the restore process to fail.
		// Preallocate might return an error if the filesystem (implementation) does not
		// support preallocation or our parameters combination to the preallocate call
		// This should yield a syscall.ENOTSUP error, but some other errors might also
		// show up.
		debug.Log("Failed to preallocate %v with size %v: %v", tmpFile.Name(), currentSize, err)
	}

	buf := make([]byte, chunker.MaxSize)
	for bid, offsets := range existingOffsetsByBlobID {
		id := bid.String()
		ci := fileChunks.Chunks[id]
		buf = buf[:ci.Length]
		_, err = f.ReadAt(buf, ci.Offset)
		if err != nil {
			return nil, 0, false, err
		}

		for _, offset1 := range offsets {
			_, err = tmpFile.WriteAt(buf, offset1)
			if err != nil {
				return nil, 0, false, err
			}
		}
	}

	err = tmpFile.Close()
	if err != nil {
		return nil, 0, false, err
	}

	err = f.Close()
	if err != nil {
		return nil, 0, false, err
	}

	// move tmpFile to target
	err = os.Rename(tmpFile.Name(), target)
	if err != nil {
		return nil, 0, false, err
	}

	return existingBlobs, currentSize, len(existingBlobs) == len(node.Content), nil
}

func offsetsEqual(localOffsets, snapshotOffsets []int64) bool {
	var short, long []int64
	if len(localOffsets) < len(snapshotOffsets) {
		short = localOffsets
		long = snapshotOffsets
	} else {
		short = snapshotOffsets
		long = localOffsets
	}

	for i, offset := range short {
		if offset != long[i] {
			return false
		}
	}

	return true
}

// fileChanged tries to detect whether a file's content has changed compared
// to the contents of the node, which describes the same path in the snapshot.
// It should only be run for regular files.
func fileChanged(fi os.FileInfo, node *restic.Node, quickChangeCheck bool) bool {
	if !quickChangeCheck {
		return true
	}

	switch {
	case node == nil:
		return true
	case node.Type != "file":
		// We're only called for regular files, so this is a type change.
		return true
	case uint64(fi.Size()) != node.Size:
		return true
	case !fi.ModTime().Equal(node.ModTime):
		return true
	}

	return false
}
