package main

import (
	"context"

	internalchunker "github.com/restic/restic/internal/chunker"
	"github.com/restic/restic/internal/errors"
	"github.com/spf13/cobra"
)

var cmdChunk = &cobra.Command{
	Use:   "chunk [flags]",
	Short: "Calculate file chunks",
	Long: `
The "chunk" command calculates the chunks of all files in a directory,
and stores the result in a yaml file in /tmp/file_chunks.yaml.

If the --watch flag is set, the command will watch the directory for
changes, and update the file chunks accordingly.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runChunk(cmd.Context(), chunkOptions, globalOptions)
	},
}

// ChunkOptions collects all options for the chunk command.
type ChunkOptions struct {
	Target string
	Watch  bool
}

var chunkOptions ChunkOptions

func init() {
	cmdRoot.AddCommand(cmdChunk)

	flags := cmdChunk.Flags()
	flags.StringVarP(&chunkOptions.Target, "target", "t", "", "directory to calculate file chunks")
	flags.BoolVarP(&chunkOptions.Watch, "watch", "w", false, "watch the directory for changes")
}

func runChunk(ctx context.Context, opts ChunkOptions, gopts GlobalOptions) error {
	if opts.Target == "" {
		return errors.Fatal("please specify a directory to calculate file chunks (--target)")
	}

	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}

	err = internalchunker.CalculateAllFileChunks(ctx, repo, opts.Target, opts.Watch)
	if err != nil {
		return err
	}

	return nil
}
