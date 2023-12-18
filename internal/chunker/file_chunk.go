package chunker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/restic/chunker"
	"github.com/restic/restic/internal/crypto"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

func CalculateAllFileChunks(ctx context.Context, repo restic.Repository, target string, watch bool) (string, error) {
	var err error
	if !filepath.IsAbs(target) {
		target, err = filepath.Abs(target)
		if err != nil {
			return "", err
		}
	}

	var watcher *fsnotify.Watcher
	if watch {
		watcher, err = fsnotify.NewWatcher()
		if err != nil {
			return "", err
		}
		defer func() {
			_ = watcher.Close()
		}()
	}

	pol := repo.Config().ChunkerPolynomial
	key := repo.Key()
	// create one chunker which is reused for each file (because it contains a rather large buffer)
	ck := chunker.New(nil, pol)
	fileChunkInfos := make(FileChunkInfoMap)
	err = filepath.Walk(target, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if watcher != nil {
				return watcher.Add(path)
			}
			return nil
		}

		fci, err := calculateFileChunks(ctx, ck, pol, path, info)
		if err != nil {
			return err
		}

		if fci != nil {
			fileChunkInfos[path] = fci
		}

		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}

	path, err := SaveFileChunks(key, fileChunkInfos)
	if err != nil {
		return "", err
	}

	if !watch {
		return path, nil
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

					err = updateFileChunks(ctx, event.Name, path, fileInfo, false, ck, pol, key)
					if err != nil {
						fmt.Printf("DEBUG: can not update file %s: %v\n", event.Name, err)
						return err
					}
				} else if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
					err = updateFileChunks(ctx, event.Name, path, nil, true, ck, pol, key)
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

	return path, err
}

func updateFileChunks(ctx context.Context, name, chunkFile string, info os.FileInfo, deleted bool, ck *chunker.Chunker, pol chunker.Pol, key *crypto.Key) error {
	fileChunkInfos := FileChunkInfoMap{}
	fileChunkInfosPtr, err := ReadFileChunks(chunkFile, key)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if fileChunkInfosPtr != nil {
		fileChunkInfos = *fileChunkInfosPtr
	}

	if deleted {
		fmt.Printf("DEBUG: delete file %s\n", name)
		delete(fileChunkInfos, name)
	} else {
		fci, err := calculateFileChunks(ctx, ck, pol, name, info)
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

	_, err = SaveFileChunks(key, fileChunkInfos, chunkFile)
	return err
}

func calculateFileChunks(ctx context.Context, ck *chunker.Chunker, pol chunker.Pol, name string, info os.FileInfo) (*FileChunkInfo, error) {
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
		Chunks: make(map[string]ChunkInfo),
	}

	// reuse the chunker
	ck.Reset(f, pol)
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

		id := restic.Hash(chunk.Data).String()
		length := int64(chunk.Length)
		// we only record the first chunk with the same id in a file
		if _, ok := fileChunkInfo.Chunks[id]; !ok {
			fileChunkInfo.Chunks[id] = ChunkInfo{
				Length: length,
				Offset: offset,
			}
		}

		fileChunkInfo.OffSets = append(fileChunkInfo.OffSets, offset)

		offset += length
	}

	return fileChunkInfo, nil
}

func ReadFileChunks(path string, key *crypto.Key) (*FileChunkInfoMap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	} else if len(data) == 0 {
		return &FileChunkInfoMap{}, nil
	}

	nonce, ciphertext := data[:key.NonceSize()], data[key.NonceSize():]
	plaintext, err := key.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	res := &FileChunkInfoMap{}
	err = yaml.Unmarshal(plaintext, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func SaveFileChunks(key *crypto.Key, fileChunkInfos FileChunkInfoMap, chunkFile ...string) (string, error) {
	pathFormat := "/tmp/file_chunks_%s.yaml"

	if fileChunkInfos == nil {
		return "", nil
	}

	data, err := yaml.Marshal(fileChunkInfos)
	if err != nil {
		return "", err
	}

	ciphertext := crypto.NewBlobBuffer(len(data))
	ciphertext = ciphertext[:0]
	nonce := crypto.NewRandomNonce()
	ciphertext = append(ciphertext, nonce...)

	ciphertext = key.Seal(ciphertext, nonce, data, nil)

	var path string
	if len(chunkFile) > 0 {
		path = chunkFile[0]
	} else {
		path = fmt.Sprintf(pathFormat, restic.Hash(ciphertext).String()[0:8])
	}

	err = os.WriteFile(path, ciphertext, 0644)
	if err != nil {
		return "", err
	}

	return path, nil
}
