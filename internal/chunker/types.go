package chunker

import (
	"github.com/restic/restic/internal/restic"
)

type FileChunkInfo struct {
	Name    string
	Size    int64
	Chunks  map[restic.ID]ChunkInfo
	OffSets []int64
}

type ChunkInfo struct {
	ID     restic.ID
	Length int64
	Offset int64
}

type FileChunkInfoMap map[string]*FileChunkInfo
