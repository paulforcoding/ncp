package main

import (
	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/mkfiles"
)

// runMkfiles handles `ncp mkfiles <dir>`.
func runMkfiles(cmd *cobra.Command, args []string) error {
	num, _ := cmd.Flags().GetInt("num")
	minSize, _ := cmd.Flags().GetInt64("minsize")
	maxSize, _ := cmd.Flags().GetInt64("maxsize")
	maxDirDepth, _ := cmd.Flags().GetInt("maxdirdepth")
	dir := args[0]

	gen, err := mkfiles.NewGenerator(mkfiles.Config{
		Dir:         dir,
		NumFiles:    num,
		MinSize:     minSize,
		MaxSize:     maxSize,
		MaxDirDepth: maxDirDepth,
	})
	if err != nil {
		return err
	}

	return gen.Run()
}
