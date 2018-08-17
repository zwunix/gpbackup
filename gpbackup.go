// +build gpbackup

package main

import (
	. "github.com/greenplum-db/gpbackup/backup"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:     "gpbackup",
		Short:   "gpbackup is the parallel backup utility for Greenplum",
		Args:    cobra.NoArgs,
		Version: GetVersion(),
		Run: func(cmd *cobra.Command, args []string) {
			defer DoTeardown()
			DoFlagValidation(cmd)
			DoSetup()
			DoBackup()
		}}
	DoInit(rootCmd)
	rootCmd.GenBashCompletionFile("/tmp/gpbackup_bash_comp")
}
