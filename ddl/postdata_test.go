package ddl_test

import (
	"github.com/greenplum-db/gpbackup/ddl"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
)

var _ = Describe("backup/postdata tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "postdata")
	})
	Context("PrintCreateIndexStatements", func() {
		It("can print a basic index", func() {
			indexes := []ddl.IndexDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateIndexStatements(backupfile, toc, indexes, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testindex", "INDEX")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);`)
		})
		It("can print an index used for clustering", func() {
			indexes := []ddl.IndexDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)", IsClustered: true}}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateIndexStatements(backupfile, toc, indexes, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testindex", "INDEX")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);
ALTER TABLE public.testtable CLUSTER ON testindex;`)
		})
		It("can print an index with a tablespace", func() {
			indexes := []ddl.IndexDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Tablespace: "test_tablespace", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateIndexStatements(backupfile, toc, indexes, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);
ALTER INDEX public.testindex SET TABLESPACE test_tablespace;`)
		})
		It("can print an index with a comment", func() {
			indexes := []ddl.IndexDefinition{{Oid: 1, Name: "testindex", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE INDEX testindex ON public.testtable USING btree(i)"}}
			indexMetadataMap := ddl.MetadataMap{1: {Comment: "This is an index comment."}}
			ddl.PrintCreateIndexStatements(backupfile, toc, indexes, indexMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE INDEX testindex ON public.testtable USING btree(i);

COMMENT ON INDEX public.testindex IS 'This is an index comment.';`)
		})
	})
	Context("PrintCreateRuleStatements", func() {
		It("can print a basic rule", func() {
			rules := []ddl.QuerySimpleDefinition{{Oid: 1, Name: "testrule", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"}}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateRuleStatements(backupfile, toc, rules, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testrule", "RULE")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;`)
		})
		It("can print a rule with a comment", func() {
			rules := []ddl.QuerySimpleDefinition{{Oid: 1, Name: "testrule", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;"}}
			ruleMetadataMap := ddl.MetadataMap{1: {Comment: "This is a rule comment."}}
			ddl.PrintCreateRuleStatements(backupfile, toc, rules, ruleMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE RULE update_notify AS ON UPDATE TO testtable DO NOTIFY testtable;

COMMENT ON RULE testrule ON public.testtable IS 'This is a rule comment.';`)
		})
	})
	Context("PrintCreateTriggerStatements", func() {
		It("can print a basic trigger", func() {
			triggers := []ddl.QuerySimpleDefinition{{Oid: 1, Name: "testtrigger", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}}
			emptyMetadataMap := ddl.MetadataMap{}
			ddl.PrintCreateTriggerStatements(backupfile, toc, triggers, emptyMetadataMap)
			testutils.ExpectEntry(toc.PostdataEntries, 0, "public", "public.testtable", "testtrigger", "TRIGGER")
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();`)
		})
		It("can print a trigger with a comment", func() {
			triggers := []ddl.QuerySimpleDefinition{{Oid: 1, Name: "testtrigger", OwningSchema: "public", OwningTable: "testtable", Def: "CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger()"}}
			triggerMetadataMap := ddl.MetadataMap{1: {Comment: "This is a trigger comment."}}
			ddl.PrintCreateTriggerStatements(backupfile, toc, triggers, triggerMetadataMap)
			testutils.AssertBufferContents(toc.PostdataEntries, buffer, `CREATE TRIGGER sync_testtable AFTER INSERT OR DELETE OR UPDATE ON testtable FOR EACH STATEMENT EXECUTE PROCEDURE flatfile_update_trigger();

COMMENT ON TRIGGER testtrigger ON public.testtable IS 'This is a trigger comment.';`)
		})
	})
})
