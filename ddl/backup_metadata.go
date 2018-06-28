package ddl

import (
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

var (
	connectionPool *dbconn.DBConn
	globalTOC *utils.TOC
	ObjectCounts map[string]int
	includeRelations []string
	excludeRelations []string
	includeSchemas []string
	excludeSchemas []string
	leafPartitionData bool
)

func InitializeDDLInfo(connection *dbconn.DBConn, toc *utils.TOC, includeRelationsList []string, excludeRelationsList []string,
	includeSchemasList []string, excludeSchemasList []string, leafPartData bool) {
	connectionPool = connection
	globalTOC = toc
	includeRelations = includeRelationsList
	excludeRelations = excludeRelationsList
	includeSchemas = includeSchemasList
	excludeSchemas = excludeSchemasList
	leafPartitionData = leafPartData
	ObjectCounts = make(map[string]int, 0)
}

func BackupGlobal(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	BackupTablespaces(metadataFile)
	BackupCreateDatabase(metadataFile)
	BackupDatabaseGUCs(metadataFile)

	if len(includeSchemas) == 0 {
		BackupResourceQueues(metadataFile)
		if connectionPool.Version.AtLeast("5") {
			BackupResourceGroups(metadataFile)
		}
		BackupRoles(metadataFile)
		BackupRoleGrants(metadataFile)
	}
}

func BackupPredata(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	gplog.Info("Writing pre-data metadata")

	BackupSchemas(metadataFile)
	if len(includeSchemas) == 0 && connectionPool.Version.AtLeast("5") {
		BackupExtensions(metadataFile)
	}

	if connectionPool.Version.AtLeast("6") {
		BackupCollations(metadataFile)
	}
	procLangs := GetProceduralLanguages(connectionPool)
	langFuncs, otherFuncs, functionMetadata := RetrieveFunctions(procLangs)
	types, typeMetadata, funcInfoMap := RetrieveTypes()

	if len(includeSchemas) == 0 {
		BackupProceduralLanguages(metadataFile, procLangs, langFuncs, functionMetadata, funcInfoMap)
	}

	BackupShellTypes(metadataFile, types)
	if connectionPool.Version.AtLeast("5") {
		BackupEnumTypes(metadataFile, typeMetadata)
	}

	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)
	sequences, sequenceOwnerColumns := RetrieveSequences()
	BackupCreateSequences(metadataFile, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints()

	BackupFunctionsAndTypesAndTables(metadataFile, otherFuncs, types, tables, functionMetadata, typeMetadata, relationMetadata, tableDefs, constraints)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceOwnerColumns)

	if len(includeSchemas) == 0 {
		BackupProtocols(metadataFile, funcInfoMap)
		if connectionPool.Version.AtLeast("6") {
			BackupForeignDataWrappers(metadataFile, funcInfoMap)
			BackupForeignServers(metadataFile)
			BackupUserMappings(metadataFile)
		}
	}

	if connectionPool.Version.AtLeast("5") {
		BackupTSParsers(metadataFile)
		BackupTSTemplates(metadataFile)
		BackupTSDictionaries(metadataFile)
		BackupTSConfigurations(metadataFile)
	}

	BackupOperators(metadataFile)
	if connectionPool.Version.AtLeast("5") {
		BackupOperatorFamilies(metadataFile)
	}
	BackupOperatorClasses(metadataFile)

	BackupConversions(metadataFile)
	BackupAggregates(metadataFile, funcInfoMap)
	BackupCasts(metadataFile)
	BackupViews(metadataFile, relationMetadata)
	BackupConstraints(metadataFile, constraints, conMetadata)
}

func BackupRelationPredata(connectionPool *dbconn.DBConn, metadataFile *utils.FileWithByteCount, tables []Relation, tableDefs map[uint32]TableDefinition) {
	gplog.Info("Writing table metadata")

	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)

	sequences, sequenceOwnerColumns := RetrieveSequences()
	BackupCreateSequences(metadataFile, sequences, relationMetadata)

	constraints, conMetadata := RetrieveConstraints(tables...)

	BackupTables(metadataFile, tables, relationMetadata, tableDefs, constraints)
	PrintAlterSequenceStatements(metadataFile, globalTOC, sequences, sequenceOwnerColumns)

	BackupViews(metadataFile, relationMetadata)

	BackupConstraints(metadataFile, constraints, conMetadata)
	gplog.Info("Table metadata backup complete")
}

func BackupPostdata(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing post-data metadata")

	BackupIndexes(metadataFile)
	BackupRules(metadataFile)
	BackupTriggers(metadataFile)
}