package backup

import (
	"flag"
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	connection *utils.DBConn
	logger     *utils.Logger
)

var ( // Command-line flags
	dbname      = flag.String("dbname", "", "The database to be backed up")
	debug       = flag.Bool("debug", false, "Print verbose and debug log messages")
	dumpDir     = flag.String("dumpdir", "", "The directory to which all dump files will be written")
	quiet       = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose     = flag.Bool("verbose", false, "Print verbose log messages")
	dumpGlobals = flag.Bool("globals", false, "Dump global metadata")
)

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gpbackup", ""))
}

func SetLogger(log *utils.Logger) {
	logger = log
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	flag.Parse()
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	if *quiet {
		logger.SetVerbosity(utils.LOGERROR)
	} else if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
	connection = utils.NewDBConn(*dbname)
	connection.Connect()
	connection.Exec("SET application_name TO 'gpbackup'")

	utils.SetDumpTimestamp("")

	if *dumpDir != "" {
		utils.BaseDumpDir = *dumpDir
	}
	logger.Verbose("Creating dump directories")
	segConfig := utils.GetSegmentConfiguration(connection)
	utils.SetupSegmentConfiguration(segConfig)
	utils.CreateDumpDirs()
}

func DoBackup() {
	logger.Info("Dump Key = %s", utils.DumpTimestamp)
	logger.Info("Dump Database = %s", utils.QuoteIdent(connection.DBName))
	logger.Info("Database Size = %s", connection.GetDBSize())

	masterDumpDir := utils.GetDirForContent(-1)

	globalFilename := fmt.Sprintf("%s/global.sql", masterDumpDir)
	predataFilename := fmt.Sprintf("%s/predata.sql", masterDumpDir)
	postdataFilename := fmt.Sprintf("%s/postdata.sql", masterDumpDir)

	connection.Begin()
	connection.Exec("SET search_path TO pg_catalog")

	tables := GetAllUserTables(connection)
	extTableMap := GetExternalTablesMap(connection)

	logger.Info("Writing global database metadata to %s", globalFilename)
	backupGlobal(globalFilename)
	logger.Info("Global database metadata dump complete")

	logger.Info("Writing pre-data metadata to %s", predataFilename)
	backupPredata(predataFilename, tables, extTableMap)
	logger.Info("Pre-data metadata dump complete")

	logger.Info("Writing data to file")
	backupData(tables, extTableMap)
	logger.Info("Data dump complete")

	logger.Info("Writing post-data metadata to %s", postdataFilename)
	backupPostdata(postdataFilename, tables, extTableMap)
	logger.Info("Post-data metadata dump complete")

	connection.Commit()
}

func backupGlobal(filename string) {
	globalFile := utils.MustOpenFile(filename)

	logger.Verbose("Writing session GUCs to global file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(globalFile, gucs)

	logger.Verbose("Writing CREATE TABLESPACE statements to global file")
	tablespaces := GetTablespaces(connection)
	tablespaceMetadata := GetMetadataForObjectType(connection, TablespaceParams)
	PrintCreateTablespaceStatements(globalFile, tablespaces, tablespaceMetadata)

	logger.Verbose("Writing CREATE DATABASE statement to global file")
	dbnames := GetDatabaseNames(connection)
	dbMetadata := GetMetadataForObjectType(connection, DatabaseParams)
	PrintCreateDatabaseStatement(globalFile, connection.DBName, dbnames, dbMetadata, *dumpGlobals)

	logger.Verbose("Writing database GUCs to global file")
	databaseGucs := GetDatabaseGUCs(connection)
	PrintDatabaseGUCs(globalFile, databaseGucs, connection.DBName)

	logger.Verbose("Writing CREATE RESOURCE QUEUE statements to global file")
	resQueues := GetResourceQueues(connection)
	resQueueMetadata := GetCommentsForObjectType(connection, ResQueueParams)
	PrintCreateResourceQueueStatements(globalFile, resQueues, resQueueMetadata)

	logger.Verbose("Writing CREATE ROLE statements to global file")
	roles := GetRoles(connection)
	roleMetadata := GetCommentsForObjectType(connection, RoleParams)
	PrintCreateRoleStatements(globalFile, roles, roleMetadata)

	logger.Verbose("Writing GRANT ROLE statements to global file")
	roleMembers := GetRoleMembers(connection)
	PrintRoleMembershipStatements(globalFile, roleMembers)
}

func backupPredata(filename string, tables []Relation, extTableMap map[string]bool) {
	predataFile := utils.MustOpenFile(filename)
	PrintConnectionString(predataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(predataFile, gucs)

	logger.Verbose("Writing CREATE SCHEMA statements to predata file")
	schemas := GetAllUserSchemas(connection)
	schemaMetadata := GetMetadataForObjectType(connection, SchemaParams)
	PrintCreateSchemaStatements(predataFile, schemas, schemaMetadata)

	types := GetTypeDefinitions(connection)
	typeMetadata := GetMetadataForObjectType(connection, TypeParams)
	functions := GetFunctionDefinitions(connection)
	funcInfoMap := GetFunctionOidToInfoMap(connection)
	functionMetadata := GetMetadataForObjectType(connection, FunctionParams)
	types, functions = ConstructDependencyLists(connection, types, functions)

	logger.Verbose("Writing CREATE TYPE statements for shell types to predata file")
	PrintCreateShellTypeStatements(predataFile, types)

	logger.Verbose("Writing CREATE PROCEDURAL LANGUAGE statements to predata file")
	procLangs := GetProceduralLanguages(connection)
	langFuncs, otherFuncs := ExtractLanguageFunctions(functions, procLangs)
	for _, langFunc := range langFuncs {
		PrintCreateFunctionStatement(predataFile, langFunc, functionMetadata[langFunc.Oid])
	}
	procLangMetadata := GetMetadataForObjectType(connection, ProcLangParams)
	PrintCreateLanguageStatements(predataFile, procLangs, funcInfoMap, procLangMetadata)

	logger.Verbose("Writing CREATE TYPE statements for enum types to predata file")
	PrintCreateEnumTypeStatements(predataFile, types, typeMetadata)

	logger.Verbose("Writing CREATE FUNCTION statements and CREATE TYPE statements for base, composite, and domain types to predata file")
	sortedSlice := SortFunctionsAndTypesInDependencyOrder(types, otherFuncs)
	filteredMetadata := ConstructFunctionAndTypeMetadataMap(typeMetadata, functionMetadata)
	PrintCreateDependentTypeAndFunctionStatements(predataFile, sortedSlice, filteredMetadata)

	logger.Verbose("Writing CREATE PROTOCOL statements to predata file")
	protocols := GetExternalProtocols(connection)
	protoMetadata := GetMetadataForObjectType(connection, ProtocolParams)
	PrintCreateExternalProtocolStatements(predataFile, protocols, funcInfoMap, protoMetadata)

	logger.Verbose("Writing CREATE OPERATOR statements to predata file")
	operators := GetOperators(connection)
	operatorMetadata := GetMetadataForObjectType(connection, OperatorParams)
	PrintCreateOperatorStatements(predataFile, operators, operatorMetadata)

	logger.Verbose("Writing CREATE OPERATOR FAMILY statements to predata file")
	operatorFamilies := GetOperatorFamilies(connection)
	operatorFamilyMetadata := GetMetadataForObjectType(connection, OperatorFamilyParams)
	PrintCreateOperatorFamilyStatements(predataFile, operatorFamilies, operatorFamilyMetadata)

	logger.Verbose("Writing CREATE OPERATOR CLASS statements to predata file")
	operatorClasses := GetOperatorClasses(connection)
	operatorClassMetadata := GetMetadataForObjectType(connection, OperatorClassParams)
	PrintCreateOperatorClassStatements(predataFile, operatorClasses, operatorClassMetadata)

	logger.Verbose("Writing CREATE AGGREGATE statements to predata file")
	aggDefs := GetAggregateDefinitions(connection)
	aggMetadata := GetMetadataForObjectType(connection, AggregateParams)
	PrintCreateAggregateStatements(predataFile, aggDefs, funcInfoMap, aggMetadata)

	logger.Verbose("Writing CREATE CAST statements to predata file")
	castDefs := GetCastDefinitions(connection)
	castMetadata := GetCommentsForObjectType(connection, CastParams)
	PrintCreateCastStatements(predataFile, castDefs, castMetadata)

	relationMetadata := GetMetadataForObjectType(connection, RelationParams)

	logger.Verbose("Writing CREATE SEQUENCE statements to predata file")
	sequenceDefs := GetAllSequences(connection)
	PrintCreateSequenceStatements(predataFile, sequenceDefs, relationMetadata)

	logger.Verbose("Writing CREATE TABLE statements to predata file")
	tables = ConstructTableDependencies(connection, tables)
	tables = SortRelations(tables)
	for _, table := range tables {
		isExternal := extTableMap[table.ToString()]
		tableDef := ConstructDefinitionsForTable(connection, table, isExternal)
		PrintCreateTableStatement(predataFile, table, tableDef, relationMetadata[table.RelationOid])
	}

	logger.Verbose("Writing ALTER SEQUENCE statements to predata file")
	sequenceColumnOwners := GetSequenceColumnOwnerMap(connection)
	PrintAlterSequenceStatements(predataFile, sequenceDefs, sequenceColumnOwners)

	logger.Verbose("Writing CREATE VIEW statements to predata file")
	views := GetViewDefinitions(connection)
	views = ConstructViewDependencies(connection, views)
	views = SortViews(views)
	PrintCreateViewStatements(predataFile, views, relationMetadata)

	logger.Verbose("Writing ADD CONSTRAINT statements to predata file")
	constraints := GetConstraints(connection)
	conMetadata := GetCommentsForObjectType(connection, ConParams)
	PrintConstraintStatements(predataFile, constraints, conMetadata)
}

func backupData(tables []Relation, extTableMap map[string]bool) {
	for _, table := range tables {
		isExternal := extTableMap[table.ToString()]
		if !isExternal {
			logger.Verbose("Writing data for table %s to file", table.ToString())
			dumpFile := GetTableDumpFilePath(table)
			CopyTableOut(connection, table, dumpFile)
		} else {
			logger.Warn("Skipping data dump of table %s because it is an external table.", table.ToString())
		}
	}
	logger.Verbose("Writing table map file to %s", GetTableMapFilePath())
	WriteTableMapFile(tables)
}

func backupPostdata(filename string, tables []Relation, extTableMap map[string]bool) {
	postdataFile := utils.MustOpenFile(filename)
	PrintConnectionString(postdataFile, connection.DBName)

	logger.Verbose("Writing session GUCs to predata file")
	gucs := GetSessionGUCs(connection)
	PrintSessionGUCs(postdataFile, gucs)

	logger.Verbose("Writing CREATE INDEX statements to postdata file")
	indexNameMap := ConstructImplicitIndexNames(connection)
	indexes := GetIndexDefinitions(connection, indexNameMap)
	indexMetadata := GetCommentsForObjectType(connection, IndexParams)
	PrintCreateIndexStatements(postdataFile, indexes, indexMetadata)

	logger.Verbose("Writing CREATE RULE statements to postdata file")
	rules := GetRuleDefinitions(connection)
	ruleMetadata := GetCommentsForObjectType(connection, RuleParams)
	PrintCreateRuleStatements(postdataFile, rules, ruleMetadata)

	logger.Verbose("Writing CREATE TRIGGER statements to postdata file")
	triggers := GetTriggerDefinitions(connection)
	triggerMetadata := GetCommentsForObjectType(connection, TriggerParams)
	PrintCreateTriggerStatements(postdataFile, triggers, triggerMetadata)
}

func DoTeardown() {
	if r := recover(); r != nil {
		fmt.Println(r)
	}
	if connection != nil {
		connection.Close()
	}
	// TODO: Add logic for error codes based on whether we Abort()ed or not
}
