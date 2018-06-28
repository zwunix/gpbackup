package ddl_test

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/structmatcher"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/ddl"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/lib/pq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_shared tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintConstraintStatements", func() {
		var (
			uniqueOne        ddl.Constraint
			uniqueTwo        ddl.Constraint
			primarySingle    ddl.Constraint
			primaryComposite ddl.Constraint
			foreignOne       ddl.Constraint
			foreignTwo       ddl.Constraint
			emptyMetadataMap ddl.MetadataMap
		)
		BeforeEach(func() {
			uniqueOne = ddl.Constraint{Oid: 1, Name: "tablename_i_key", ConType: "u", ConDef: "UNIQUE (i)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			uniqueTwo = ddl.Constraint{Oid: 0, Name: "tablename_j_key", ConType: "u", ConDef: "UNIQUE (j)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			primarySingle = ddl.Constraint{Oid: 0, Name: "tablename_pkey", ConType: "p", ConDef: "PRIMARY KEY (i)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			primaryComposite = ddl.Constraint{Oid: 0, Name: "tablename_pkey", ConType: "p", ConDef: "PRIMARY KEY (i, j)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			foreignOne = ddl.Constraint{Oid: 0, Name: "tablename_i_fkey", ConType: "f", ConDef: "FOREIGN KEY (i) REFERENCES other_tablename(a)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			foreignTwo = ddl.Constraint{Oid: 0, Name: "tablename_j_fkey", ConType: "f", ConDef: "FOREIGN KEY (j) REFERENCES other_tablename(b)", OwningObject: "public.tablename", IsDomainConstraint: false, IsPartitionParent: false}
			emptyMetadataMap = ddl.MetadataMap{}
		})

		Context("No constraints", func() {
			It("doesn't print anything", func() {
				constraints := []ddl.Constraint{}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testhelper.NotExpectRegexp(buffer, `CONSTRAINT`)
			})
		})
		Context("Constraints involving different columns", func() {
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint with a comment", func() {
				constraints := []ddl.Constraint{uniqueOne}
				constraintMetadataMap := testutils.DefaultMetadataMap("CONSTRAINT", false, false, true)
				ddl.PrintConstraintStatements(backupfile, toc, constraints, constraintMetadataMap)
				testutils.ExpectEntry(toc.PredataEntries, 0, "", "public.tablename", "tablename_i_key", "CONSTRAINT")
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);


COMMENT ON CONSTRAINT tablename_i_key ON public.tablename IS 'This is a constraint comment.';`)
			})
			It("prints an ADD CONSTRAINT statement for one UNIQUE constraint", func() {
				constraints := []ddl.Constraint{uniqueOne}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
			It("prints ADD CONSTRAINT statements for two UNIQUE constraints", func() {
				constraints := []ddl.Constraint{uniqueOne, uniqueTwo}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);`)
			})
			It("prints an ADD CONSTRAINT statement for one PRIMARY KEY constraint on one column", func() {
				constraints := []ddl.Constraint{primarySingle}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`)
			})
			It("prints an ADD CONSTRAINT statement for one composite PRIMARY KEY constraint on two columns", func() {
				constraints := []ddl.Constraint{primaryComposite}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`)
			})
			It("prints an ADD CONSTRAINT statement for one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignOne}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for two FOREIGN KEY constraints", func() {
				constraints := []ddl.Constraint{foreignOne, foreignTwo}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignTwo, uniqueOne}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignTwo, primarySingle}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
			It("prints ADD CONSTRAINT statements for one two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignTwo, primaryComposite}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);`)
			})
		})
		Context("Constraints involving the same column", func() {
			It("prints ADD CONSTRAINT statements for one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignOne, uniqueOne}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignOne, primarySingle}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("prints ADD CONSTRAINT statements for a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []ddl.Constraint{foreignOne, primaryComposite}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);`,
					`ALTER TABLE ONLY public.tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);`)
			})
			It("doesn't print an ADD CONSTRAINT statement for domain check constraint", func() {
				domainCheckConstraint := ddl.Constraint{Oid: 0, Name: "check1", ConType: "c", ConDef: "CHECK (VALUE <> 42::numeric)", OwningObject: "public.domain1", IsDomainConstraint: true, IsPartitionParent: false}
				constraints := []ddl.Constraint{domainCheckConstraint}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testhelper.NotExpectRegexp(buffer, `ALTER DOMAIN`)
			})
			It("prints an ADD CONSTRAINT statement for a parent partition table", func() {
				uniqueOne.IsPartitionParent = true
				constraints := []ddl.Constraint{uniqueOne}
				ddl.PrintConstraintStatements(backupfile, toc, constraints, emptyMetadataMap)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);`)
			})
		})
	})
	Describe("PrintCreateSchemaStatements", func() {
		It("can print a basic schema", func() {
			schemas := []ddl.Schema{{Oid: 0, Name: "schemaname"}}
			emptyMetadataMap := ddl.MetadataMap{}

			ddl.PrintCreateSchemaStatements(backupfile, toc, schemas, emptyMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "schemaname", "", "schemaname", "SCHEMA")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, "CREATE SCHEMA schemaname;")
		})
		It("can print a schema with privileges, an owner, and a comment", func() {
			schemas := []ddl.Schema{{Oid: 1, Name: "schemaname"}}
			schemaMetadataMap := testutils.DefaultMetadataMap("SCHEMA", true, true, true)

			ddl.PrintCreateSchemaStatements(backupfile, toc, schemas, schemaMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SCHEMA schemaname;

COMMENT ON SCHEMA schemaname IS 'This is a schema comment.';


ALTER SCHEMA schemaname OWNER TO testrole;


REVOKE ALL ON SCHEMA schemaname FROM PUBLIC;
REVOKE ALL ON SCHEMA schemaname FROM testrole;
GRANT ALL ON SCHEMA schemaname TO testrole;`)
		})
	})
	Describe("SchemaFromString", func() {
		It("can parse an unquoted string", func() {
			testString := `schemaname`
			newSchema := ddl.SchemaFromString(testString)
			Expect(newSchema.Oid).To(Equal(uint32(0)))
			Expect(newSchema.Name).To(Equal(`schemaname`))
		})
		It("can parse a quoted string", func() {
			testString := `"schema,name"`
			newSchema := ddl.SchemaFromString(testString)
			Expect(newSchema.Oid).To(Equal(uint32(0)))
			Expect(newSchema.Name).To(Equal(`schema,name`))
		})
		It("panics if given an invalid string", func() {
			testString := `schema.name`
			defer testhelper.ShouldPanicWithMessage(`schema.name is not a valid identifier`)
			ddl.SchemaFromString(testString)
		})
	})
	Describe("GetUniqueSchemas", func() {
		alphabeticalAFoo := ddl.Relation{SchemaOid: 1, Oid: 0, Schema: "otherschema", Name: "foo", DependsUpon: nil, Inherits: nil}
		alphabeticalABar := ddl.Relation{SchemaOid: 1, Oid: 0, Schema: "otherschema", Name: "bar", DependsUpon: nil, Inherits: nil}
		schemaOther := ddl.Schema{Oid: 2, Name: "otherschema"}
		alphabeticalBFoo := ddl.Relation{SchemaOid: 2, Oid: 0, Schema: "public", Name: "foo", DependsUpon: nil, Inherits: nil}
		alphabeticalBBar := ddl.Relation{SchemaOid: 2, Oid: 0, Schema: "public", Name: "bar", DependsUpon: nil, Inherits: nil}
		schemaPublic := ddl.Schema{Oid: 1, Name: "public"}
		schemas := []ddl.Schema{schemaOther, schemaPublic}

		It("has multiple tables in a single schema", func() {
			tables := []ddl.Relation{alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := ddl.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]ddl.Schema{schemaPublic}))
		})
		It("has multiple schemas, each with multiple tables", func() {
			tables := []ddl.Relation{alphabeticalBFoo, alphabeticalBBar, alphabeticalAFoo, alphabeticalABar}
			uniqueSchemas := ddl.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]ddl.Schema{schemaOther, schemaPublic}))
		})
		It("has no tables", func() {
			tables := []ddl.Relation{}
			uniqueSchemas := ddl.GetUniqueSchemas(schemas, tables)
			Expect(uniqueSchemas).To(Equal([]ddl.Schema{}))
		})
	})
	Describe("PrintObjectMetadata", func() {
		hasAllPrivileges := testutils.DefaultACLForType("anothertestrole", "TABLE")
		hasMostPrivileges := testutils.DefaultACLForType("testrole", "TABLE")
		hasMostPrivileges.Trigger = false
		hasSinglePrivilege := ddl.ACL{Grantee: "", Trigger: true}
		hasAllPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("anothertestrole", "TABLE")
		hasMostPrivilegesWithGrant := testutils.DefaultACLForTypeWithGrant("testrole", "TABLE")
		hasMostPrivilegesWithGrant.TriggerWithGrant = false
		hasSinglePrivilegeWithGrant := ddl.ACL{Grantee: "", TriggerWithGrant: true}
		privileges := []ddl.ACL{hasAllPrivileges, hasMostPrivileges, hasSinglePrivilege}
		privilegesWithGrant := []ddl.ACL{hasAllPrivilegesWithGrant, hasMostPrivilegesWithGrant, hasSinglePrivilegeWithGrant}
		It("prints a block with a table comment", func() {
			tableMetadata := ddl.ObjectMetadata{Comment: "This is a table comment."}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';`)
		})
		It("prints a block with a table comment with special characters", func() {
			tableMetadata := ddl.ObjectMetadata{Comment: `This is a ta'ble 1+=;,./\>,<@\\n^comment.`}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a ta''ble 1+=;,./\>,<@\\n^comment.';`)
		})
		It("prints an ALTER TABLE ... OWNER TO statement to set the table owner", func() {
			tableMetadata := ddl.ObjectMetadata{Owner: "testrole"}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints a block of REVOKE and GRANT statements", func() {
			tableMetadata := ddl.ObjectMetadata{Privileges: privileges}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints a block of REVOKE and GRANT statements WITH GRANT OPTION", func() {
			tableMetadata := ddl.ObjectMetadata{Privileges: privilegesWithGrant}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole WITH GRANT OPTION;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC WITH GRANT OPTION;`)
		})
		It("prints a block of REVOKE and GRANT statements, some with WITH GRANT OPTION, some without", func() {
			tableMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{hasAllPrivileges, hasMostPrivilegesWithGrant}}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole WITH GRANT OPTION;`)
		})
		It("prints both an ALTER TABLE ... OWNER TO statement and a table comment", func() {
			tableMetadata := ddl.ObjectMetadata{Comment: "This is a table comment.", Owner: "testrole"}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;`)
		})
		It("prints both a block of REVOKE and GRANT statements and an ALTER TABLE ... OWNER TO statement", func() {
			tableMetadata := ddl.ObjectMetadata{Privileges: privileges, Owner: "testrole"}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints both a block of REVOKE and GRANT statements and a table comment", func() {
			tableMetadata := ddl.ObjectMetadata{Privileges: privileges, Comment: "This is a table comment."}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints REVOKE and GRANT statements, an ALTER TABLE ... OWNER TO statement, and comments", func() {
			tableMetadata := ddl.ObjectMetadata{Privileges: privileges, Owner: "testrole", Comment: "This is a table comment."}
			ddl.PrintObjectMetadata(backupfile, tableMetadata, "public.tablename", "TABLE")
			testhelper.ExpectRegexp(buffer, `

COMMENT ON TABLE public.tablename IS 'This is a table comment.';


ALTER TABLE public.tablename OWNER TO testrole;


REVOKE ALL ON TABLE public.tablename FROM PUBLIC;
REVOKE ALL ON TABLE public.tablename FROM testrole;
GRANT ALL ON TABLE public.tablename TO anothertestrole;
GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON TABLE public.tablename TO testrole;
GRANT TRIGGER ON TABLE public.tablename TO PUBLIC;`)
		})
		It("prints SERVER for ALTER and FOREIGN SERVER for GRANT/REVOKE for a foreign server", func() {
			serverPrivileges := testutils.DefaultACLForType("testrole", "FOREIGN SERVER")
			serverMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{serverPrivileges}, Owner: "testrole"}
			ddl.PrintObjectMetadata(backupfile, serverMetadata, "foreignserver", "FOREIGN SERVER")
			testhelper.ExpectRegexp(buffer, `

ALTER SERVER foreignserver OWNER TO testrole;


REVOKE ALL ON FOREIGN SERVER foreignserver FROM PUBLIC;
REVOKE ALL ON FOREIGN SERVER foreignserver FROM testrole;
GRANT ALL ON FOREIGN SERVER foreignserver TO testrole;`)
		})
	})
	Describe("ConstructMetadataMap", func() {
		object1A := ddl.MetadataQueryStruct{Oid: 1, Privileges: sql.NullString{String: "gpadmin=r/gpadmin", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object1B := ddl.MetadataQueryStruct{Oid: 1, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: ""}
		object2 := ddl.MetadataQueryStruct{Oid: 2, Privileges: sql.NullString{String: "testrole=r/testrole", Valid: true}, Kind: "", Owner: "testrole", Comment: "this is a comment"}
		objectDefaultKind := ddl.MetadataQueryStruct{Oid: 3, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Default", Owner: "testrole", Comment: ""}
		objectEmptyKind := ddl.MetadataQueryStruct{Oid: 4, Privileges: sql.NullString{String: "", Valid: false}, Kind: "Empty", Owner: "testrole", Comment: ""}
		var metadataList []ddl.MetadataQueryStruct
		BeforeEach(func() {
			metadataList = []ddl.MetadataQueryStruct{}
		})
		It("No objects", func() {
			metadataMap := ddl.ConstructMetadataMap(metadataList)
			Expect(len(metadataMap)).To(Equal(0))
		})
		It("One object", func() {
			metadataList = []ddl.MetadataQueryStruct{object2}
			metadataMap := ddl.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment"}
			Expect(len(metadataMap)).To(Equal(1))
			Expect(metadataMap[2]).To(Equal(expectedObjectMetadata))
		})
		It("One object with two ACL entries", func() {
			metadataList = []ddl.MetadataQueryStruct{object1A, object1B}
			metadataMap := ddl.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			Expect(len(metadataMap)).To(Equal(1))
			Expect(metadataMap[1]).To(Equal(expectedObjectMetadata))
		})
		It("Multiple objects", func() {
			metadataList = []ddl.MetadataQueryStruct{object1A, object1B, object2}
			metadataMap := ddl.ConstructMetadataMap(metadataList)
			expectedObjectMetadataOne := ddl.ObjectMetadata{Privileges: []ddl.ACL{{Grantee: "gpadmin", Select: true}, {Grantee: "testrole", Select: true}}, Owner: "testrole"}
			expectedObjectMetadataTwo := ddl.ObjectMetadata{Privileges: []ddl.ACL{{Grantee: "testrole", Select: true}}, Owner: "testrole", Comment: "this is a comment"}
			Expect(len(metadataMap)).To(Equal(2))
			Expect(metadataMap[1]).To(Equal(expectedObjectMetadataOne))
			Expect(metadataMap[2]).To(Equal(expectedObjectMetadataTwo))
		})
		It("Default Kind", func() {
			metadataList = []ddl.MetadataQueryStruct{objectDefaultKind}
			metadataMap := ddl.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{}, Owner: "testrole"}
			Expect(len(metadataMap)).To(Equal(1))
			Expect(metadataMap[3]).To(Equal(expectedObjectMetadata))
		})
		It("'Empty' Kind", func() {
			metadataList = []ddl.MetadataQueryStruct{objectEmptyKind}
			metadataMap := ddl.ConstructMetadataMap(metadataList)
			expectedObjectMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{{Grantee: "GRANTEE"}}, Owner: "testrole"}
			Expect(len(metadataMap)).To(Equal(1))
			Expect(metadataMap[4]).To(Equal(expectedObjectMetadata))
		})
	})
	Describe("ParseACL", func() {
		It("parses an ACL string representing default privileges", func() {
			aclStr := ""
			result := ddl.ParseACL(aclStr)
			Expect(result).To(BeNil())
		})
		It("parses an ACL string representing no privileges", func() {
			aclStr := "GRANTEE=/GRANTOR"
			expected := ddl.ACL{Grantee: "GRANTEE"}
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with multiple privileges", func() {
			aclStr := "testrole=arwdDxt/gpadmin"
			expected := testutils.DefaultACLForType("testrole", "TABLE")
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with one privilege", func() {
			aclStr := "testrole=a/gpadmin"
			expected := ddl.ACL{Grantee: "testrole", Insert: true}
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role name with special characters", func() {
			aclStr := `"test|role"=a/gpadmin`
			expected := ddl.ACL{Grantee: `test|role`, Insert: true}
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with some privileges with GRANT and some without including GRANT", func() {
			aclStr := "testrole=ar*w*d*tXUCTc/gpadmin"
			expected := ddl.ACL{Grantee: "testrole", Insert: true, SelectWithGrant: true, UpdateWithGrant: true,
				DeleteWithGrant: true, Trigger: true, Execute: true, Usage: true, Create: true, Temporary: true, Connect: true}
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string containing a role with all privileges including GRANT", func() {
			aclStr := "testrole=a*D*x*t*X*U*C*T*c*/gpadmin"
			expected := ddl.ACL{Grantee: "testrole", InsertWithGrant: true, TruncateWithGrant: true, ReferencesWithGrant: true,
				TriggerWithGrant: true, ExecuteWithGrant: true, UsageWithGrant: true, CreateWithGrant: true, TemporaryWithGrant: true, ConnectWithGrant: true}
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
		It("parses an ACL string granting privileges to PUBLIC", func() {
			aclStr := "=a/gpadmin"
			expected := ddl.ACL{Grantee: "", Insert: true}
			result := ddl.ParseACL(aclStr)
			structmatcher.ExpectStructsToMatch(&expected, result)
		})
	})
	Describe("PrintCreateDependentTypeAndFunctionAndTablesStatements", func() {
		var (
			objects      []ddl.Sortable
			metadataMap  ddl.MetadataMap
			tableDefsMap map[uint32]ddl.TableDefinition
		)
		BeforeEach(func() {
			objects = []ddl.Sortable{
				ddl.Function{Oid: 1, Schema: "public", Name: "function", FunctionBody: "SELECT $1 + $2",
					Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer", Language: "sql"},
				ddl.Type{Oid: 2, Schema: "public", Name: "base", Type: "b", Input: "typin", Output: "typout", Category: "U"},
				ddl.Type{Oid: 3, Schema: "public", Name: "composite", Type: "c", Attributes: pq.StringArray{"\tfoo integer"}, Category: "U"},
				ddl.Type{Oid: 4, Schema: "public", Name: "domain", Type: "d", BaseType: "numeric", Category: "U"},
				ddl.Relation{Oid: 5, Schema: "public", Name: "relation"},
			}
			metadataMap = ddl.MetadataMap{
				1: ddl.ObjectMetadata{Comment: "function"},
				2: ddl.ObjectMetadata{Comment: "base type"},
				3: ddl.ObjectMetadata{Comment: "composite type"},
				4: ddl.ObjectMetadata{Comment: "domain"},
				5: ddl.ObjectMetadata{Comment: "relation"},
			}
			tableDefsMap = map[uint32]ddl.TableDefinition{
				5: {DistPolicy: "DISTRIBUTED RANDOMLY", ColumnDefs: []ddl.ColumnDefinition{}},
			}
		})
		It("prints create statements for dependent types, functions, and tables (domain has a constraint)", func() {
			constraints := []ddl.Constraint{
				{Name: "check_constraint", ConDef: "CHECK (VALUE > 2)", OwningObject: "public.domain"},
			}
			ddl.PrintCreateDependentTypeAndFunctionAndTablesStatements(backupfile, toc, objects, metadataMap, tableDefsMap, constraints)
			testhelper.ExpectRegexp(buffer, `
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric
	CONSTRAINT check_constraint CHECK (VALUE > 2);


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';
`)
		})
		It("prints create statements for dependent types, functions, and tables (no domain constraint)", func() {
			constraints := []ddl.Constraint{}
			ddl.PrintCreateDependentTypeAndFunctionAndTablesStatements(backupfile, toc, objects, metadataMap, tableDefsMap, constraints)
			testhelper.ExpectRegexp(buffer, `
CREATE FUNCTION public.function(integer, integer) RETURNS integer AS
$_$SELECT $1 + $2$_$
LANGUAGE sql;


COMMENT ON FUNCTION public.function(integer, integer) IS 'function';


CREATE TYPE public.base (
	INPUT = typin,
	OUTPUT = typout
);


COMMENT ON TYPE public.base IS 'base type';


CREATE TYPE public.composite AS (
	foo integer
);

COMMENT ON TYPE public.composite IS 'composite type';

CREATE DOMAIN public.domain AS numeric;


COMMENT ON DOMAIN public.domain IS 'domain';


CREATE TABLE public.relation (
) DISTRIBUTED RANDOMLY;


COMMENT ON TABLE public.relation IS 'relation';
`)
		})
	})
})
