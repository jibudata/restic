package chunker

type FileChunkInfo struct {
	Name    string
	Size    int64
	Chunks  map[string]ChunkInfo
	OffSets []int64
}

type ChunkInfo struct {
	Length int64
	Offset int64
}

type FileChunkInfoMap map[string]*FileChunkInfo
