package chunker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/restic/chunker"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

func CalculateAllFileChunks(ctx context.Context, repo restic.Repository, target string, watch bool) error {
	var err error
	if !filepath.IsAbs(target) {
		target, err = filepath.Abs(target)
		if err != nil {
			return err
		}
	}

	var watcher *fsnotify.Watcher
	if watch {
		watcher, err = fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		defer func() {
			_ = watcher.Close()
		}()
	}

	pol := repo.Config().ChunkerPolynomial
	fileChunkInfos := make(FileChunkInfos)
	err = filepath.Walk(target, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && watcher != nil {
			return watcher.Add(path)
		}

		fci, err := calculateFileChunks(ctx, pol, path, info)
		if err != nil {
			return err
		}

		if fci != nil {
			fileChunkInfos[path] = fci
		}

		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	data, err := yaml.Marshal(fileChunkInfos)
	if err != nil {
		return err
	}

	err = os.WriteFile("/tmp/file_chunks.yaml", data, 0644)
	if err != nil {
		return err
	}

	if !watch {
		return nil
	}

	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case event, ok := <-watcher.Events:
				if !ok {
					return nil
				}

				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
					fileInfo, err := os.Stat(event.Name)
					if err != nil {
						fmt.Printf("DEBUG: can not stat file %s: %v\n", event.Name, err)
						return err
					}

					err = updateFileChunks(ctx, pol, event.Name, fileInfo, false)
					if err != nil {
						fmt.Printf("DEBUG: can not update file %s: %v\n", event.Name, err)
						return err
					}
				} else if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
					err = updateFileChunks(ctx, pol, event.Name, nil, true)
					if err != nil {
						fmt.Printf("DEBUG: can not update file %s: %v\n", event.Name, err)
						return err
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return nil
				}

				fmt.Printf("DEBUG: file watcher error: %v\n", err)
				return err
			}
		}
	})

	err = eg.Wait()

	return err
}

func updateFileChunks(ctx context.Context, pol chunker.Pol, name string, info os.FileInfo, deleted bool) error {
	fileChunkInfos := FileChunkInfos{}
	data, err := os.ReadFile("/tmp/file_chunks.yaml")
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if len(data) > 0 {
		err = yaml.Unmarshal(data, &fileChunkInfos)
		if err != nil {
			return err
		}
	}

	if deleted {
		fmt.Printf("DEBUG: delete file %s\n", name)
		delete(fileChunkInfos, name)
	} else {
		fci, err := calculateFileChunks(ctx, pol, name, info)
		if err != nil {
			return err
		}

		fmt.Printf("DEBUG: create or update file %s\n", name)
		if fci != nil {
			fileChunkInfos[name] = fci
		} else {
			delete(fileChunkInfos, name)
		}
	}

	data, err = yaml.Marshal(fileChunkInfos)
	if err != nil {
		return err
	}

	err = os.WriteFile("/tmp/file_chunks.yaml", data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func calculateFileChunks(ctx context.Context, pol chunker.Pol, name string, info os.FileInfo) (*FileChunkInfo, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if !info.Mode().IsRegular() {
		return nil, nil
	}

	// skip small files
	if info.Size() < 2*chunker.MinSize {
		return nil, nil
	}

	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = f.Close()
	}()

	fileChunkInfo := &FileChunkInfo{
		Name:   name,
		Size:   info.Size(),
		Chunks: make(map[restic.ID]ChunkInfo),
	}

	ck := chunker.New(f, pol)
	buf := make([]byte, chunker.MaxSize)
	var offset int64
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		chunk, err := ck.Next(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		id := restic.Hash(chunk.Data)
		length := int64(chunk.Length)
		// we only record the first chunk with the same id in a file
		if _, ok := fileChunkInfo.Chunks[id]; !ok {
			fileChunkInfo.Chunks[id] = ChunkInfo{
				ID:     id,
				Length: length,
				Offset: offset,
			}
		}

		fileChunkInfo.OffSets = append(fileChunkInfo.OffSets, offset)

		offset += length
	}

	return fileChunkInfo, nil
}
