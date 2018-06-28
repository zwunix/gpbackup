package backup

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/greenplum-db/gpbackup/ddl"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and printing metadata, so that the logic for each object type
 * can all be in one place and backup.go can serve as a high-level look at the
 * overall backup flow.
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

func InitializeConnectionPool() {
	connectionPool = dbconn.NewDBConnFromEnvironment(*dbname)
	connectionPool.MustConnect(*numJobs)
	utils.SetDatabaseVersion(connectionPool)
	ddl.InitializeMetadataParams(connectionPool)
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		connectionPool.MustExec("SET application_name TO 'gpbackup'", connNum)
		connectionPool.MustBegin(connNum)
		SetSessionGUCs(connNum)
	}
}

func SetSessionGUCs(connNum int) {
	// These GUCs ensure the dumps portability across systems
	connectionPool.MustExec("SET search_path TO pg_catalog", connNum)
	connectionPool.MustExec("SET statement_timeout = 0", connNum)
	connectionPool.MustExec("SET DATESTYLE = ISO", connNum)
	if connectionPool.Version.AtLeast("5") {
		connectionPool.MustExec("SET synchronize_seqscans TO off", connNum)
	}
	if connectionPool.Version.AtLeast("6") {
		connectionPool.MustExec("SET INTERVALSTYLE = POSTGRES", connNum)
	}
}

func InitializeBackupReport() {
	dbname := dbconn.MustSelectString(connectionPool, fmt.Sprintf("select quote_ident(datname) AS string FROM pg_database where datname='%s'", connectionPool.DBName))
	config := utils.BackupConfig{
		DatabaseName:    dbname,
		DatabaseVersion: connectionPool.Version.VersionString,
		BackupVersion:   version,
	}
	isIncludeSchemaFiltered := len(*includeSchemas) > 0
	isIncludeTableFiltered := len(*includeRelations) > 0
	isExcludeSchemaFiltered := len(*excludeSchemas) > 0
	isExcludeTableFiltered := len(*excludeRelations) > 0
	dbSize := ""
	if !*metadataOnly && !isIncludeSchemaFiltered && !isIncludeTableFiltered && !isExcludeSchemaFiltered && !isExcludeTableFiltered {
		gplog.Verbose("Getting database size")
		dbSize = ddl.GetDBSize(connectionPool)
	}

	backupReport = &utils.Report{
		DatabaseSize: dbSize,
		BackupConfig: config,
	}
	utils.InitializeCompressionParameters(!*noCompression, *compressionLevel)
	backupReport.SetBackupParamsFromFlags(*dataOnly, *metadataOnly, "", isIncludeSchemaFiltered, isIncludeTableFiltered, isExcludeSchemaFiltered, isExcludeTableFiltered, *singleDataFile, *withStats)
	backupReport.ConstructBackupParamsString()
}

func InitializeFilterLists() {
	if *excludeRelationFile != "" {
		*excludeRelations = iohelper.MustReadLinesFromFile(*excludeRelationFile)
	}
	if *includeRelationFile != "" {
		*includeRelations = iohelper.MustReadLinesFromFile(*includeRelationFile)
	}
}

func CreateBackupDirectoriesOnAllHosts() {
	remoteOutput := globalCluster.GenerateAndExecuteCommand("Creating backup directories", func(contentID int) string {
		return fmt.Sprintf("mkdir -p %s", globalFPInfo.GetDirForContent(contentID))
	}, cluster.ON_SEGMENTS_AND_MASTER)
	globalCluster.CheckClusterError(remoteOutput, "Unable to create backup directories", func(contentID int) string {
		return fmt.Sprintf("Unable to create backup directory %s", globalFPInfo.GetDirForContent(contentID))
	})
}


/*
 * Generic metadata wrapper functions
 */

func LogBackupInfo() {
	gplog.Info("Backup Timestamp = %s", globalFPInfo.Timestamp)
	gplog.Info("Backup Database = %s", connectionPool.DBName)
	params := strings.Split(backupReport.BackupParamsString, "\n")
	for _, param := range params {
		gplog.Verbose(param)
	}
}

/*
 * Data wrapper functions
 */

func BackupStatistics(statisticsFile *utils.FileWithByteCount, tables []ddl.Relation) {
	attStats := GetAttributeStatistics(connectionPool, tables)
	tupleStats := GetTupleStatistics(connectionPool, tables)

	ddl.BackupSessionGUCs(statisticsFile)
	PrintStatisticsStatements(statisticsFile, globalTOC, tables, attStats, tupleStats)
}

func BackupIncrementalMetadata() {
	gplog.Verbose("Writing incremental metadata to the TOC")
	aoTableEntries := ddl.GetAOIncrementalMetadata(connectionPool)
	globalTOC.IncrementalMetadata.AO = aoTableEntries
}
