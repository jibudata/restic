package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/walker"
)

var cmdLc = &cobra.Command{
	Use:   "lc [flags] checkpointID [dir...]",
	Short: "List files in a checkpoint",
	Long: `
The "lc" command lists files and directories in a checkpoint.

The special checkpoint ID "latest" can be used to list files and
directories of the latest checkpoint in the repository. The
--host flag can be used in conjunction to select the latest
checkpoint originating from a certain host only.

File listings can optionally be filtered by directories. Any
positional arguments after the checkpoint ID are interpreted as
absolute directory paths, and only files inside those directories
will be listed. If the --recursive flag is used, then the filter
will allow traversing into matching directories' subfolders.
Any directory paths specified must be absolute (starting with
a path separator); paths use the forward slash '/' as separator.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runLc(lcOptions, globalOptions, args)
	},
}

// LcOptions collects all options for the ls command.
type LcOptions struct {
	ListLong  bool
	Hosts     []string
	Tags      restic.TagLists
	Paths     []string
	Recursive bool
}

var lcOptions LcOptions

func init() {
	cmdRoot.AddCommand(cmdLc)

	flags := cmdLc.Flags()
	flags.BoolVarP(&lcOptions.ListLong, "long", "l", false, "use a long listing format showing size and mode")
	flags.StringArrayVarP(&lcOptions.Hosts, "host", "H", nil, "only consider checkpoints for this `host`, when checkpoint ID \"latest\" is given (can be specified multiple times)")
	flags.Var(&lcOptions.Tags, "tag", "only consider checkpoints which include this `taglist`, when checkpoint ID \"latest\" is given (can be specified multiple times)")
	flags.StringArrayVar(&lcOptions.Paths, "path", nil, "only consider checkpoints which include this (absolute) `path`, when checkpoint ID \"latest\" is given (can be specified multiple times)")
	flags.BoolVar(&lcOptions.Recursive, "recursive", false, "include files in subfolders of the listed directories")
}

type lscheckpoint struct {
	*restic.Checkpoint
	ID         *restic.ID `json:"id"`
	ShortID    string     `json:"short_id"`
	StructType string     `json:"struct_type"` // "checkpoint"
}

// Print node in our custom JSON format, followed by a newline.
func lcNodeJSON(enc *json.Encoder, path string, node *restic.Node) error {
	n := &struct {
		Name        string      `json:"name"`
		Type        string      `json:"type"`
		Path        string      `json:"path"`
		UID         uint32      `json:"uid"`
		GID         uint32      `json:"gid"`
		Size        *uint64     `json:"size,omitempty"`
		Mode        os.FileMode `json:"mode,omitempty"`
		Permissions string      `json:"permissions,omitempty"`
		ModTime     time.Time   `json:"mtime,omitempty"`
		AccessTime  time.Time   `json:"atime,omitempty"`
		ChangeTime  time.Time   `json:"ctime,omitempty"`
		StructType  string      `json:"struct_type"` // "node"

		size uint64 // Target for Size pointer.
	}{
		Name:        node.Name,
		Type:        node.Type,
		Path:        path,
		UID:         node.UID,
		GID:         node.GID,
		size:        node.Size,
		Mode:        node.Mode,
		Permissions: node.Mode.String(),
		ModTime:     node.ModTime,
		AccessTime:  node.AccessTime,
		ChangeTime:  node.ChangeTime,
		StructType:  "node",
	}
	// Always print size for regular files, even when empty,
	// but never for other types.
	if node.Type == "file" {
		n.Size = &n.size
	}

	return enc.Encode(n)
}

func runLc(opts LcOptions, gopts GlobalOptions, args []string) error {
	if len(args) == 0 {
		return errors.Fatal("no checkpoint ID specified, specify checkpoint ID or use special ID 'latest'")
	}

	// extract any specific directories to walk
	var dirs []string
	if len(args) > 1 {
		dirs = args[1:]
		for _, dir := range dirs {
			if !strings.HasPrefix(dir, "/") {
				return errors.Fatal("All path filters must be absolute, starting with a forward slash '/'")
			}
		}
	}

	withinDir := func(nodepath string) bool {
		if len(dirs) == 0 {
			return true
		}

		for _, dir := range dirs {
			// we're within one of the selected dirs, example:
			//   nodepath: "/test/foo"
			//   dir:      "/test"
			if fs.HasPathPrefix(dir, nodepath) {
				return true
			}
		}
		return false
	}

	approachingMatchingTree := func(nodepath string) bool {
		if len(dirs) == 0 {
			return true
		}

		for _, dir := range dirs {
			// the current node path is a prefix for one of the
			// directories, so we're interested in something deeper in the
			// tree. Example:
			//   nodepath: "/test"
			//   dir:      "/test/foo"
			if fs.HasPathPrefix(nodepath, dir) {
				return true
			}
		}
		return false
	}

	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}

	if err = repo.LoadIndex(gopts.ctx); err != nil {
		Verbosef("load index failed: %v):\n", err)
		return err
	}

	ctx, cancel := context.WithCancel(gopts.ctx)
	defer cancel()

	var (
		printCheckpoint func(sn *restic.Checkpoint)
		printNode       func(path string, node *restic.Node)
	)

	if gopts.JSON {
		enc := json.NewEncoder(gopts.stdout)

		printCheckpoint = func(sn *restic.Checkpoint) {
			err := enc.Encode(lscheckpoint{
				Checkpoint: sn,
				ID:         sn.ID(),
				ShortID:    sn.ID().Str(),
				StructType: "Checkpoint",
			})
			if err != nil {
				Warnf("JSON encode failed: %v\n", err)
			}
		}

		printNode = func(path string, node *restic.Node) {
			err := lcNodeJSON(enc, path, node)
			if err != nil {
				Warnf("JSON encode failed: %v\n", err)
			}
		}
	} else {
		printCheckpoint = func(sn *restic.Checkpoint) {
			Verbosef("Checkpoint %s of %v filtered by %v at %s):\n", sn.ID().Str(), sn.Paths, dirs, sn.Time)
		}
		printNode = func(path string, node *restic.Node) {
			Printf("%s\n", formatNode(path, node, lcOptions.ListLong))
		}
	}

	for sn := range FindFilteredCheckpoints(ctx, repo, opts.Hosts, opts.Tags, opts.Paths, args[:1]) {
		Verbosef("Checkpoint %v):\n", sn.ID().Str())
		printCheckpoint(sn)

		err := walker.Walk(ctx, repo, *sn.Tree, nil, func(_ restic.ID, nodepath string, node *restic.Node, err error) (bool, error) {
			if err != nil {
				return false, err
			}
			if node == nil {
				return false, nil
			}

			if withinDir(nodepath) {
				// if we're within a dir, print the node
				printNode(nodepath, node)

				// if recursive listing is requested, signal the walker that it
				// should continue walking recursively
				if opts.Recursive {
					return false, nil
				}
			}

			// if there's an upcoming match deeper in the tree (but we're not
			// there yet), signal the walker to descend into any subdirs
			if approachingMatchingTree(nodepath) {
				return false, nil
			}

			// otherwise, signal the walker to not walk recursively into any
			// subdirs
			if node.Type == "dir" {
				return false, walker.ErrSkipNode
			}
			return false, nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}
