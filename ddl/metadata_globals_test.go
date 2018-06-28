package ddl_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/ddl"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/metadata_globals tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "global")
	})
	Describe("PrintSessionGUCs", func() {
		It("prints session GUCs", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			gucs := ddl.SessionGUCs{ClientEncoding: "UTF8"}

			ddl.PrintSessionGUCs(backupfile, toc, gucs)
			testhelper.ExpectRegexp(buffer, `SET client_encoding = 'UTF8';
`)
		})
	})
	Describe("PrintCreateDatabaseStatement", func() {
		It("prints a basic CREATE DATABASE statement", func() {
			db := ddl.Database{Oid: 1, Name: "testdb", Tablespace: "pg_default"}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateDatabaseStatement(backupfile, toc, db, emptyMetadataMap)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "testdb", "DATABASE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb TEMPLATE template0;`)
		})
		It("prints a CREATE DATABASE statement for a reserved keyword named database", func() {
			db := ddl.Database{Oid: 1, Name: `"table"`, Tablespace: "pg_default"}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateDatabaseStatement(backupfile, toc, db, emptyMetadataMap)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", `"table"`, "DATABASE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE "table" TEMPLATE template0;`)
		})
		It("prints a CREATE DATABASE statement with privileges, an owner, and a comment", func() {
			dbMetadataMap := testutils.DefaultMetadataMap("DATABASE", true, true, true)
			dbMetadata := dbMetadataMap[1]
			dbMetadata.Privileges[0].Create = false
			dbMetadataMap[1] = dbMetadata
			db := ddl.Database{Oid: 1, Name: "testdb", Tablespace: "pg_default"}
			ddl.PrintCreateDatabaseStatement(backupfile, toc, db, dbMetadataMap)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb TEMPLATE template0;`,
				`COMMENT ON DATABASE testdb IS 'This is a database comment.';


ALTER DATABASE testdb OWNER TO testrole;


REVOKE ALL ON DATABASE testdb FROM PUBLIC;
REVOKE ALL ON DATABASE testdb FROM testrole;
GRANT TEMPORARY,CONNECT ON DATABASE testdb TO testrole;`)
		})
		It("prints a CREATE DATABASE statement with all modifiers", func() {
			db := ddl.Database{Oid: 1, Name: "testdb", Tablespace: "test_tablespace", Encoding: "UTF8", Collate: "en_US.utf-8", CType: "en_US.utf-8"}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateDatabaseStatement(backupfile, toc, db, emptyMetadataMap)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE DATABASE testdb TEMPLATE template0 TABLESPACE test_tablespace ENCODING 'UTF8' LC_COLLATE 'en_US.utf-8' LC_CTYPE 'en_US.utf-8';`)
		})
	})
	Describe("PrintDatabaseGUCs", func() {
		dbname := "testdb"
		defaultOidGUC := "SET default_with_oids TO 'true'"
		searchPathGUC := "SET search_path TO pg_catalog, public"
		defaultStorageGUC := "SET gp_default_storage_options TO 'appendonly=true,blocksize=32768'"

		It("prints single database GUC", func() {
			gucs := []string{defaultOidGUC}

			ddl.PrintDatabaseGUCs(backupfile, toc, gucs, dbname)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "testdb", "DATABASE GUC")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `ALTER DATABASE testdb SET default_with_oids TO 'true';`)
		})
		It("prints multiple database GUCs", func() {
			gucs := []string{defaultOidGUC, searchPathGUC, defaultStorageGUC}

			ddl.PrintDatabaseGUCs(backupfile, toc, gucs, dbname)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`ALTER DATABASE testdb SET default_with_oids TO 'true';`,
				`ALTER DATABASE testdb SET search_path TO pg_catalog, public;`,
				`ALTER DATABASE testdb SET gp_default_storage_options TO 'appendonly=true,blocksize=32768';`)
		})
	})
	Describe("PrintCreateResourceQueueStatements", func() {
		var emptyResQueueMetadata = map[uint32]ddl.ObjectMetadata{}
		It("prints resource queues", func() {
			someQueue := ddl.ResourceQueue{Oid: 1, Name: "some_queue", ActiveStatements: 1, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			maxCostQueue := ddl.ResourceQueue{Oid: 1, Name: `"someMaxCostQueue"`, ActiveStatements: -1, MaxCost: "99.9", CostOvercommit: true, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []ddl.ResourceQueue{someQueue, maxCostQueue}

			ddl.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "some_queue", "RESOURCE QUEUE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`CREATE RESOURCE QUEUE some_queue WITH (ACTIVE_STATEMENTS=1);`,
				`CREATE RESOURCE QUEUE "someMaxCostQueue" WITH (MAX_COST=99.9, COST_OVERCOMMIT=TRUE);`)
		})
		It("prints a resource queue with active statements and max cost", func() {
			someActiveMaxCostQueue := ddl.ResourceQueue{Oid: 1, Name: `"someActiveMaxCostQueue"`, ActiveStatements: 5, MaxCost: "62.03", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []ddl.ResourceQueue{someActiveMaxCostQueue}

			ddl.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "someActiveMaxCostQueue" WITH (ACTIVE_STATEMENTS=5, MAX_COST=62.03);`)
		})
		It("prints a resource queue with all properties", func() {
			everythingQueue := ddl.ResourceQueue{Oid: 1, Name: `"everythingQueue"`, ActiveStatements: 7, MaxCost: "32.80", CostOvercommit: true, MinCost: "1.34", Priority: "low", MemoryLimit: "2GB"}
			resQueues := []ddl.ResourceQueue{everythingQueue}

			ddl.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "everythingQueue" WITH (ACTIVE_STATEMENTS=7, MAX_COST=32.80, COST_OVERCOMMIT=TRUE, MIN_COST=1.34, PRIORITY=LOW, MEMORY_LIMIT='2GB');`)
		})
		It("prints a resource queue with a comment", func() {
			commentQueue := ddl.ResourceQueue{Oid: 1, Name: `"commentQueue"`, ActiveStatements: 1, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []ddl.ResourceQueue{commentQueue}
			resQueueMetadata := testutils.DefaultMetadataMap("RESOURCE QUEUE", false, false, true)

			ddl.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, resQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE RESOURCE QUEUE "commentQueue" WITH (ACTIVE_STATEMENTS=1);

COMMENT ON RESOURCE QUEUE "commentQueue" IS 'This is a resource queue comment.';`)
		})
		It("prints ALTER statement for pg_default resource queue", func() {
			pgDefault := ddl.ResourceQueue{Oid: 1, Name: "pg_default", ActiveStatements: 1, MaxCost: "-1.00", CostOvercommit: false, MinCost: "0.00", Priority: "medium", MemoryLimit: "-1"}
			resQueues := []ddl.ResourceQueue{pgDefault}

			ddl.PrintCreateResourceQueueStatements(backupfile, toc, resQueues, emptyResQueueMetadata)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `ALTER RESOURCE QUEUE pg_default WITH (ACTIVE_STATEMENTS=1);`)
		})
	})
	Describe("PrintCreateResourceGroupStatements", func() {
		var emptyResGroupMetadata = map[uint32]ddl.ObjectMetadata{}
		It("prints resource groups", func() {
			someGroup := ddl.ResourceGroup{Oid: 1, Name: "some_group", CPURateLimit: 10, MemoryLimit: 20, Concurrency: 15, MemorySharedQuota: 25, MemorySpillRatio: 30}
			someGroup2 := ddl.ResourceGroup{Oid: 2, Name: "some_group2", CPURateLimit: 20, MemoryLimit: 30, Concurrency: 25, MemorySharedQuota: 35, MemorySpillRatio: 10}
			resGroups := []ddl.ResourceGroup{someGroup, someGroup2}

			ddl.PrintCreateResourceGroupStatements(backupfile, toc, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "some_group", "RESOURCE GROUP")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`CREATE RESOURCE GROUP some_group WITH (CPU_RATE_LIMIT=10, MEMORY_LIMIT=20, MEMORY_SHARED_QUOTA=25, MEMORY_SPILL_RATIO=30, CONCURRENCY=15);`,
				`CREATE RESOURCE GROUP some_group2 WITH (CPU_RATE_LIMIT=20, MEMORY_LIMIT=30, MEMORY_SHARED_QUOTA=35, MEMORY_SPILL_RATIO=10, CONCURRENCY=25);`)
		})
		It("prints ALTER statement for default_group resource group", func() {
			default_group := ddl.ResourceGroup{Oid: 1, Name: "default_group", CPURateLimit: 10, MemoryLimit: 20, Concurrency: 15, MemorySharedQuota: 25, MemorySpillRatio: 30}
			resGroups := []ddl.ResourceGroup{default_group}

			ddl.PrintCreateResourceGroupStatements(backupfile, toc, resGroups, emptyResGroupMetadata)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "default_group", "RESOURCE GROUP")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `ALTER RESOURCE GROUP default_group SET CPU_RATE_LIMIT 10;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_LIMIT 20;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_SHARED_QUOTA 25;`,
				`ALTER RESOURCE GROUP default_group SET MEMORY_SPILL_RATIO 30;`,
				`ALTER RESOURCE GROUP default_group SET CONCURRENCY 15;`)
		})
	})
	Describe("PrintCreateRoleStatements", func() {
		testrole1 := ddl.Role{
			Oid:             1,
			Name:            "testrole1",
			Super:           false,
			Inherit:         false,
			CreateRole:      false,
			CreateDB:        false,
			CanLogin:        false,
			ConnectionLimit: -1,
			Password:        "",
			ValidUntil:      "",
			ResQueue:        "pg_default",
			ResGroup:        "default_group",
			Createrexthttp:  false,
			Createrextgpfd:  false,
			Createwextgpfd:  false,
			Createrexthdfs:  false,
			Createwexthdfs:  false,
			TimeConstraints: []ddl.TimeConstraint{},
		}

		testrole2 := ddl.Role{
			Oid:             1,
			Name:            `"testRole2"`,
			Super:           true,
			Inherit:         true,
			CreateRole:      true,
			CreateDB:        true,
			CanLogin:        true,
			ConnectionLimit: 4,
			Password:        "md5a8b2c77dfeba4705f29c094592eb3369",
			ValidUntil:      "2099-01-01 00:00:00-08",
			ResQueue:        `"testQueue"`,
			ResGroup:        `"testGroup"`,
			Createrexthttp:  true,
			Createrextgpfd:  true,
			Createwextgpfd:  true,
			Createrexthdfs:  true,
			Createwexthdfs:  true,
			TimeConstraints: []ddl.TimeConstraint{
				{
					StartDay:  0,
					StartTime: "13:30:00",
					EndDay:    3,
					EndTime:   "14:30:00",
				}, {
					StartDay:  5,
					StartTime: "00:00:00",
					EndDay:    5,
					EndTime:   "24:00:00",
				},
			},
		}
		emptyConfigMap := map[string][]string{}
		It("prints basic role", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			ddl.PrintCreateRoleStatements(backupfile, toc, []ddl.Role{testrole1}, emptyConfigMap, roleMetadataMap)

			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "testrole1", "ROLE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default RESOURCE GROUP default_group;

COMMENT ON ROLE testrole1 IS 'This is a role comment.';`)
		})
		It("prints basic role with user GUCs set", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			roleConfigMap := map[string][]string{
				"testrole1": {"SET search_path TO public", "SET client_min_messages TO 'error'", "SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none'"},
			}
			ddl.PrintCreateRoleStatements(backupfile, toc, []ddl.Role{testrole1}, roleConfigMap, roleMetadataMap)

			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "testrole1", "ROLE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default RESOURCE GROUP default_group;

ALTER ROLE testrole1 SET search_path TO public;

ALTER ROLE testrole1 SET client_min_messages TO 'error';

ALTER ROLE testrole1 SET gp_default_storage_options TO 'appendonly=true, compresslevel=6, orientation=row, compresstype=none';

COMMENT ON ROLE testrole1 IS 'This is a role comment.';`)
		})
		It("prints roles with non-defaults", func() {
			roleMetadataMap := testutils.DefaultMetadataMap("ROLE", false, false, true)
			ddl.PrintCreateRoleStatements(backupfile, toc, []ddl.Role{testrole2}, emptyConfigMap, roleMetadataMap)

			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" RESOURCE GROUP "testGroup" CREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');
ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';
ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';

COMMENT ON ROLE "testRole2" IS 'This is a role comment.';`)
		})
		It("prints multiple roles", func() {
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateRoleStatements(backupfile, toc, []ddl.Role{testrole1, testrole2}, emptyConfigMap, emptyMetadataMap)

			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`CREATE ROLE testrole1;
ALTER ROLE testrole1 WITH NOSUPERUSER NOINHERIT NOCREATEROLE NOCREATEDB NOLOGIN RESOURCE QUEUE pg_default RESOURCE GROUP default_group;`,
				`CREATE ROLE "testRole2";
ALTER ROLE "testRole2" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN CONNECTION LIMIT 4 PASSWORD 'md5a8b2c77dfeba4705f29c094592eb3369' VALID UNTIL '2099-01-01 00:00:00-08' RESOURCE QUEUE "testQueue" RESOURCE GROUP "testGroup" CREATEEXTTABLE (protocol='http') CREATEEXTTABLE (protocol='gpfdist', type='readable') CREATEEXTTABLE (protocol='gpfdist', type='writable') CREATEEXTTABLE (protocol='gphdfs', type='readable') CREATEEXTTABLE (protocol='gphdfs', type='writable');
ALTER ROLE "testRole2" DENY BETWEEN DAY 0 TIME '13:30:00' AND DAY 3 TIME '14:30:00';
ALTER ROLE "testRole2" DENY BETWEEN DAY 5 TIME '00:00:00' AND DAY 5 TIME '24:00:00';`)
		})
	})
	Describe("PrintRoleMembershipStatements", func() {
		roleWith := ddl.RoleMember{Role: "group", Member: "rolewith", Grantor: "grantor", IsAdmin: true}
		roleWithout := ddl.RoleMember{Role: "group", Member: "rolewithout", Grantor: "grantor", IsAdmin: false}
		It("prints a role without ADMIN OPTION", func() {
			ddl.PrintRoleMembershipStatements(backupfile, toc, []ddl.RoleMember{roleWithout})
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "rolewithout", "ROLE GRANT")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `GRANT group TO rolewithout GRANTED BY grantor;`)
		})
		It("prints a role WITH ADMIN OPTION", func() {
			ddl.PrintRoleMembershipStatements(backupfile, toc, []ddl.RoleMember{roleWith})
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`)
		})
		It("prints multiple roles", func() {
			ddl.PrintRoleMembershipStatements(backupfile, toc, []ddl.RoleMember{roleWith, roleWithout})
			testutils.AssertBufferContents(toc.GlobalEntries, buffer,
				`GRANT group TO rolewith WITH ADMIN OPTION GRANTED BY grantor;`,
				`GRANT group TO rolewithout GRANTED BY grantor;`)
		})
	})
	Describe("PrintCreateTablespaceStatements", func() {
		expectedTablespace := ddl.Tablespace{Oid: 1, Tablespace: "test_tablespace", FileLocation: "test_filespace"}
		It("prints a basic tablespace", func() {
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateTablespaceStatements(backupfile, toc, []ddl.Tablespace{expectedTablespace}, emptyMetadataMap)
			testutils.ExpectEntry(toc.GlobalEntries, 0, "", "", "test_tablespace", "TABLESPACE")
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;`)
		})
		It("prints a tablespace with privileges, an owner, and a comment", func() {
			tablespaceMetadataMap := testutils.DefaultMetadataMap("TABLESPACE", true, true, true)
			ddl.PrintCreateTablespaceStatements(backupfile, toc, []ddl.Tablespace{expectedTablespace}, tablespaceMetadataMap)
			testutils.AssertBufferContents(toc.GlobalEntries, buffer, `CREATE TABLESPACE test_tablespace FILESPACE test_filespace;`,
				`COMMENT ON TABLESPACE test_tablespace IS 'This is a tablespace comment.';


ALTER TABLESPACE test_tablespace OWNER TO testrole;


REVOKE ALL ON TABLESPACE test_tablespace FROM PUBLIC;
REVOKE ALL ON TABLESPACE test_tablespace FROM testrole;
GRANT ALL ON TABLESPACE test_tablespace TO testrole;`)
		})
	})
})
