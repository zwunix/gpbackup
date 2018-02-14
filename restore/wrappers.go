package restore

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and restoring metadata, so that the logic for each object type
 * can all be in one place and restore.go can serve as a high-level look at the
 * overall restore flow.
 */

/*
 * Setup and validation wrapper functions
 */

func SetLoggerVerbosity() {
	if *quiet {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if *debug {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if *verbose {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func InitializeConnection(dbname string) {
	connection = dbconn.NewDBConn(dbname)
	connection.MustConnect(*numJobs)
	utils.SetDatabaseVersion(connection)
	setupQuery := `
SET application_name TO 'gprestore';
SET search_path TO pg_catalog;
SET gp_enable_segment_copy_checking TO false;
SET gp_default_storage_options='';
SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET standard_conforming_strings = on;
SET default_with_oids = off;
`
	if connection.Version.Before("5") {
		setupQuery += "SET gp_strict_xml_parse = off;\n"
	}
	for i := 0; i < connection.NumConns; i++ {
		connection.MustExec(setupQuery, i)
	}
}

func InitializeSignalHandler() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			fmt.Println() // Add newline after "^C" is printed
			gplog.Warn("Received an interrupt, aborting restore process")
			wasTerminated = true
			DoCleanup()
			os.Exit(2)
		}
	}()
}

func InitializeBackupConfig() {
	backupConfig = utils.ReadConfigFile(globalFPInfo.GetConfigFilePath())
	utils.InitializeCompressionParameters(backupConfig.Compressed, 0)
	utils.EnsureBackupVersionCompatibility(backupConfig.BackupVersion, version)
	utils.EnsureDatabaseVersionCompatibility(backupConfig.DatabaseVersion, connection.Version)
}

func InitializeFilterLists() {
	if *includeTableFile != "" {
		includeTables = utils.ReadLinesFromFile(*includeTableFile)
	}
}

/*
 * This function is for any validation that requires a database connection but
 * does not specifically need to connect to the restore database.
 */
func DoPostgresValidation() {
	InitializeFilterLists()

	gplog.Verbose("Gathering information on backup directories")
	segConfig := cluster.GetSegmentConfiguration(connection)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := utils.ParseSegPrefix(*backupDir)
	globalFPInfo = utils.NewFilePathInfo(globalCluster.SegDirMap, *backupDir, *timestamp, segPrefix)
	VerifyBackupDirectoriesExistOnAllHosts()

	InitializeBackupConfig()
	ValidateBackupFlagCombinations()
	VerifyMetadataFilePaths(*withStats)

	tocFilename := globalFPInfo.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	globalTOC.InitializeEntryMap()

	validateFilterListsInBackupSet()
}

func ConnectToRestoreDatabase() {
	restoreDatabase := ""
	if *redirect != "" {
		restoreDatabase = *redirect
	} else {
		restoreDatabase = backupConfig.DatabaseName
	}
	InitializeConnection(restoreDatabase)
}

func DoRestoreDatabaseValidation() {
	validateFilterListsInRestoreDatabase()
}

/*
 * Metadata and/or data restore wrapper functions
 */

func GetRestoreMetadataStatements(section string, filename string, objectTypes []string, includeSchemas []string, includeTables []string) []utils.StatementWithType {
	metadataFile := utils.MustOpenFileForReading(filename)
	var statements []utils.StatementWithType
	if len(objectTypes) > 0 || len(includeSchemas) > 0 || len(includeTables) > 0 {
		statements = globalTOC.GetSQLStatementForObjectTypes(section, metadataFile, objectTypes, includeSchemas, includeTables)
	} else {
		statements = globalTOC.GetAllSQLStatements(section, metadataFile)
	}
	return statements
}

func ExecuteRestoreMetadataStatements(statements []utils.StatementWithType, objectsTitle string, progressBar utils.ProgressBar, showProgressBar int, executeInParallel bool) {
	if progressBar == nil {
		ExecuteStatementsAndCreateProgressBar(statements, objectsTitle, showProgressBar, executeInParallel)
	} else {
		ExecuteStatements(statements, progressBar, showProgressBar, executeInParallel)
	}
}

/*
 * The first time this function is called, it retrieves the session GUCs from the
 * predata file and processes them appropriately, then it returns them so they
 * can be used in later calls without the file access and processing overhead.
 */
func setGUCsForConnection(gucStatements []utils.StatementWithType, whichConn int) []utils.StatementWithType {
	if gucStatements == nil {
		objectTypes := []string{"SESSION GUCS"}
		gucStatements = GetRestoreMetadataStatements("global", globalFPInfo.GetMetadataFilePath(), objectTypes, []string{}, []string{})
	}
	ExecuteStatementsAndCreateProgressBar(gucStatements, "", utils.PB_NONE, false, whichConn)
	return gucStatements
}
