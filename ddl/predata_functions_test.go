package ddl_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/ddl"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_functions tests", func() {
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("Functions involved in printing CREATE FUNCTION statements", func() {
		var funcDef ddl.Function
		funcDefault := ddl.Function{Oid: 1, Schema: "public", Name: "func_name", ReturnsSet: false, FunctionBody: "add_two_ints", BinaryPath: "", Arguments: "integer, integer", IdentArgs: "integer, integer", ResultType: "integer", Volatility: "v", IsStrict: false, IsSecurityDefiner: false, Config: "", Cost: float32(1), NumRows: float32(0), DataAccess: "", Language: "internal", DependsUpon: nil, ExecLocation: "a"}
		BeforeEach(func() {
			funcDef = funcDefault
		})

		Describe("PrintCreateFunctionStatement", func() {
			var (
				funcMetadata ddl.ObjectMetadata
			)
			BeforeEach(func() {
				funcMetadata = ddl.ObjectMetadata{}
			})
			It("prints a function definition for an internal function without a binary path", func() {
				ddl.PrintCreateFunctionStatement(backupfile, toc, funcDef, funcMetadata)
				testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "func_name(integer, integer)", "FUNCTION")
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;`)
			})
			It("prints a function definition for a function that returns a set", func() {
				funcDef.ReturnsSet = true
				funcDef.ResultType = "SETOF integer"
				ddl.PrintCreateFunctionStatement(backupfile, toc, funcDef, funcMetadata)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS SETOF integer AS
$$add_two_ints$$
LANGUAGE internal;`)
			})
			It("prints a function definition for a function with permissions, an owner, and a comment", func() {
				funcMetadata := ddl.ObjectMetadata{Privileges: []ddl.ACL{testutils.DefaultACLForType("testrole", "FUNCTION")}, Owner: "testrole", Comment: "This is a function comment."}
				ddl.PrintCreateFunctionStatement(backupfile, toc, funcDef, funcMetadata)
				testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;


COMMENT ON FUNCTION public.func_name(integer, integer) IS 'This is a function comment.';


ALTER FUNCTION public.func_name(integer, integer) OWNER TO testrole;


REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM testrole;
GRANT ALL ON FUNCTION public.func_name(integer, integer) TO testrole;`)
			})
		})
		Describe("PrintFunctionBodyOrPath", func() {
			It("prints a function definition for an internal function with 'NULL' binary path using '-'", func() {
				funcDef.BinaryPath = "-"
				ddl.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `
$$add_two_ints$$
`)
			})
			It("prints a function definition for an internal function with a binary path", func() {
				funcDef.BinaryPath = "$libdir/binary"
				ddl.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `
'$libdir/binary', 'add_two_ints'
`)
			})
			It("prints a function definition for a function with a one-line function definition", func() {
				funcDef.FunctionBody = "SELECT $1+$2"
				funcDef.Language = "sql"
				ddl.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `$_$SELECT $1+$2$_$`)
			})
			It("prints a function definition for a function with a multi-line function definition", func() {
				funcDef.FunctionBody = `
BEGIN
	SELECT $1 + $2
END
`
				funcDef.Language = "sql"
				ddl.PrintFunctionBodyOrPath(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, `$_$
BEGIN
	SELECT $1 + $2
END
$_$`)
			})
		})
		Describe("PrintFunctionModifiers", func() {
			Context("SqlUsage cases", func() {
				It("prints 'c' as CONTAINS SQL", func() {
					funcDef.DataAccess = "c"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "CONTAINS SQL")
				})
				It("prints 'm' as MODIFIES SQL DATA", func() {
					funcDef.DataAccess = "m"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "MODIFIES SQL DATA")
				})
				It("prints 'n' as NO SQL", func() {
					funcDef.DataAccess = "n"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "NO SQL")
				})
				It("prints 'r' as READS SQL DATA", func() {
					funcDef.DataAccess = "r"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "READS SQL DATA")
				})
			})
			Context("Volatility cases", func() {
				It("does not print anything for 'v'", func() {
					funcDef.Volatility = "v"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("prints 's' as STABLE", func() {
					funcDef.Volatility = "s"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "STABLE")
				})
				It("prints 'i' as IMMUTABLE", func() {
					funcDef.Volatility = "i"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "IMMUTABLE")
				})
			})
			It("prints 'STRICT' if IsStrict is set", func() {
				funcDef.IsStrict = true
				ddl.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "STRICT")
			})
			It("prints 'SECURITY DEFINER' if IsSecurityDefiner is set", func() {
				funcDef.IsSecurityDefiner = true
				ddl.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "SECURITY DEFINER")
			})
			It("print 'WINDOW' if IsWindow is set", func() {
				funcDef.IsWindow = true
				ddl.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "WINDOW")
			})
			Context("Execlocation cases", func() {
				It("Default", func() {
					funcDef.ExecLocation = "a"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("print 'm' as EXECUTE ON MASTER", func() {
					funcDef.ExecLocation = "m"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON MASTER")
				})
				It("print 's' as EXECUTE ON ALL SEGMENTS", func() {
					funcDef.ExecLocation = "s"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "EXECUTE ON ALL SEGMENTS")
				})
			})
			Context("Cost cases", func() {
				/*
				 * The default COST values are 1 for C and internal functions and
				 * 100 for any other language, so it should not print COST clauses
				 * for those values but print any other COST.
				 */
				It("prints 'COST 5' if Cost is set to 5", func() {
					funcDef.Cost = 5
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 5")
				})
				It("prints 'COST 1' if Cost is set to 1 and language is not c or internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "sql"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 1")
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is c", func() {
					funcDef.Cost = 1
					funcDef.Language = "c"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "internal"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("prints 'COST 100' if Cost is set to 100 and language is c", func() {
					funcDef.Cost = 100
					funcDef.Language = "c"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 100")
				})
				It("prints 'COST 100' if Cost is set to 100 and language is internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "internal"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "COST 100")
				})
				It("does not print 'COST 100' if Cost is set to 100 and language is not c or internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "sql"
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			Context("NumRows cases", func() {
				/*
				 * A ROWS value of 0 means "no estimate" and 1000 means "too high
				 * to estimate", so those should not be printed but any other ROWS
				 * value should be.
				 */
				It("prints 'ROWS 5' if Rows is set to 5", func() {
					funcDef.NumRows = 5
					funcDef.ReturnsSet = true
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					testhelper.ExpectRegexp(buffer, "ROWS 5")
				})
				It("does not print 'ROWS' if Rows is set but ReturnsSet is false", func() {
					funcDef.NumRows = 100
					funcDef.ReturnsSet = false
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 0", func() {
					funcDef.NumRows = 0
					funcDef.ReturnsSet = true
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 1000", func() {
					funcDef.NumRows = 1000
					funcDef.ReturnsSet = true
					ddl.PrintFunctionModifiers(backupfile, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			It("prints config statements if any are set", func() {
				funcDef.Config = "SET client_min_messages TO error"
				ddl.PrintFunctionModifiers(backupfile, funcDef)
				testhelper.ExpectRegexp(buffer, "SET client_min_messages TO error")
			})
		})

	})
	Describe("PrintCreateAggregateStatements", func() {
		aggDefs := make([]ddl.Aggregate, 1)
		aggDefinition := ddl.Aggregate{Oid: 1, Schema: "public", Name: "agg_name", Arguments: "integer, integer", IdentArgs: "integer, integer", TransitionFunction: 1, TransitionDataType: "integer", InitValIsNull: true}
		complexAggDefinition := ddl.Aggregate{
			Schema: "public", Name: "agg_hypo_ord", Arguments: `VARIADIC "any" ORDER BY VARIADIC "any"`,
			IdentArgs: `VARIADIC "any" ORDER BY VARIADIC "any"`, TransitionFunction: 5, FinalFunction: 6,
			TransitionDataType: "internal", InitValIsNull: true, FinalFuncExtra: true, Hypothetical: true,
		}
		funcInfoMap := map[uint32]ddl.FunctionInfo{
			1: {QualifiedName: "public.mysfunc", Arguments: "integer"},
			2: {QualifiedName: "public.mypfunc", Arguments: "numeric, numeric"},
			3: {QualifiedName: "public.myffunc", Arguments: "text"},
			4: {QualifiedName: "public.mysortop", Arguments: "bigint"},
			5: {QualifiedName: "pg_catalog.ordered_set_transition_multi", Arguments: `internal, VARIADIC "any"`},
			6: {QualifiedName: "pg_catalog.rank_final", Arguments: `internal, VARIADIC "any"`},
		}
		aggMetadataMap := ddl.MetadataMap{}
		BeforeEach(func() {
			aggDefs[0] = aggDefinition
			aggMetadataMap = ddl.MetadataMap{}
		})

		It("prints an aggregate definition for an unordered aggregate with no optional specifications", func() {
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "agg_name(integer, integer)", "AGGREGATE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an ordered aggregate with no optional specifications", func() {
			aggDefs[0].IsOrdered = true
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE ORDERED AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an unordered aggregate with no arguments", func() {
			aggDefs[0].Arguments = ""
			aggDefs[0].IdentArgs = ""
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate with a preliminary function", func() {
			aggDefs[0].PreliminaryFunction = 2
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	PREFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a final function", func() {
			aggDefs[0].FinalFunction = 3
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with an initial condition", func() {
			aggDefs[0].InitialValue = "0"
			aggDefs[0].InitValIsNull = false
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	INITCOND = '0'
);`)
		})
		It("prints an aggregate with a sort operator", func() {
			aggDefs[0].SortOperator = 4
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SORTOP = public.mysortop
);`)
		})
		It("prints an aggregate with multiple specifications", func() {
			aggDefs[0].FinalFunction = 3
			aggDefs[0].SortOperator = 4
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc,
	SORTOP = public.mysortop
);`)
		})
		It("prints a hypothetical ordered-set aggregate", func() {
			aggDefs[0] = complexAggDefinition
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_hypo_ord(VARIADIC "any" ORDER BY VARIADIC "any") (
	SFUNC = pg_catalog.ordered_set_transition_multi,
	STYPE = internal,
	FINALFUNC = pg_catalog.rank_final,
	FINALFUNC_EXTRA,
	HYPOTHETICAL
);`)
		})
		It("prints an aggregate with owner and comment", func() {
			aggMetadataMap[1] = ddl.ObjectMetadata{Privileges: []ddl.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);


COMMENT ON AGGREGATE public.agg_name(integer, integer) IS 'This is an aggregate comment.';


ALTER AGGREGATE public.agg_name(integer, integer) OWNER TO testrole;`)
		})
		It("prints an aggregate with owner, comment, and no arguments", func() {
			aggDefs[0].Arguments = ""
			aggDefs[0].IdentArgs = ""
			aggMetadataMap[1] = ddl.ObjectMetadata{Privileges: []ddl.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
			ddl.PrintCreateAggregateStatements(backupfile, toc, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);


COMMENT ON AGGREGATE public.agg_name(*) IS 'This is an aggregate comment.';


ALTER AGGREGATE public.agg_name(*) OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateCastStatements", func() {
		emptyMetadataMap := ddl.MetadataMap{}
		It("prints an explicit cast with a function", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "e", CastMethod: "f"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "(src AS dst)", "CAST")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer);`)
		})
		It("prints an implicit cast with a function", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "i", CastMethod: "f"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS IMPLICIT;`)
		})
		It("prints an assignment cast with a function", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "public", FunctionName: "cast_func", FunctionArgs: "integer, integer", CastContext: "a", CastMethod: "f"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS ASSIGNMENT;`)
		})
		It("prints an explicit cast without a function", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "b"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;`)
		})
		It("prints an implicit cast without a function", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "i", CastMethod: "b"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS IMPLICIT;`)
		})
		It("prints an assignment cast without a function", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "a", CastMethod: "b"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS ASSIGNMENT;`)
		})
		It("prints an inout cast", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "i"}
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITH INOUT;`)
		})
		It("prints a cast with a comment", func() {
			castDef := ddl.Cast{Oid: 1, SourceTypeFQN: "src", TargetTypeFQN: "dst", FunctionSchema: "", FunctionName: "", FunctionArgs: "", CastContext: "e", CastMethod: "b"}
			castMetadataMap := testutils.DefaultMetadataMap("CAST", false, false, true)
			ddl.PrintCreateCastStatements(backupfile, toc, []ddl.Cast{castDef}, castMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;

COMMENT ON CAST (src AS dst) IS 'This is a cast comment.';`)
		})
	})
	Describe("PrintCreateExtensionStatement", func() {
		emptyMetadataMap := ddl.MetadataMap{}
		It("prints a create extension statement", func() {
			extensionDef := ddl.Extension{Oid: 1, Name: "extension1", Schema: "schema1"}
			ddl.PrintCreateExtensionStatements(backupfile, toc, []ddl.Extension{extensionDef}, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `SET search_path=schema1,pg_catalog;
CREATE EXTENSION IF NOT EXISTS extension1 WITH SCHEMA schema1;
SET search_path=pg_catalog;`)
		})
		It("prints a create extension statement with a comment", func() {
			extensionDef := ddl.Extension{Oid: 1, Name: "extension1", Schema: "schema1"}
			extensionMetadataMap := testutils.DefaultMetadataMap("EXTENSION", false, false, true)
			ddl.PrintCreateExtensionStatements(backupfile, toc, []ddl.Extension{extensionDef}, extensionMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `SET search_path=schema1,pg_catalog;
CREATE EXTENSION IF NOT EXISTS extension1 WITH SCHEMA schema1;
SET search_path=pg_catalog;

COMMENT ON EXTENSION extension1 IS 'This is an extension comment.';`)
		})
	})
	Describe("ExtractLanguageFunctions", func() {
		customLang1 := ddl.ProceduralLanguage{Oid: 1, Name: "custom_language", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 3, Inline: 4, Validator: 5}
		customLang2 := ddl.ProceduralLanguage{Oid: 2, Name: "custom_language2", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 5, Inline: 6, Validator: 7}
		procLangs := []ddl.ProceduralLanguage{customLang1, customLang2}
		langFunc := ddl.Function{Oid: 3, Name: "custom_handler"}
		nonLangFunc := ddl.Function{Oid: 2, Name: "random_function"}
		It("handles a case where all functions are language-associated functions", func() {
			funcDefs := []ddl.Function{langFunc}
			langFuncs, otherFuncs := ddl.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(len(langFuncs)).To(Equal(1))
			Expect(len(otherFuncs)).To(Equal(0))
			Expect(langFuncs[0].Name).To(Equal("custom_handler"))
		})
		It("handles a case where no functions are language-associated functions", func() {
			funcDefs := []ddl.Function{nonLangFunc}
			langFuncs, otherFuncs := ddl.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(len(langFuncs)).To(Equal(0))
			Expect(len(otherFuncs)).To(Equal(1))
			Expect(otherFuncs[0].Name).To(Equal("random_function"))
		})
		It("handles a case where some functions are language-associated functions", func() {
			funcDefs := []ddl.Function{langFunc, nonLangFunc}
			langFuncs, otherFuncs := ddl.ExtractLanguageFunctions(funcDefs, procLangs)
			Expect(len(langFuncs)).To(Equal(1))
			Expect(len(otherFuncs)).To(Equal(1))
			Expect(langFuncs[0].Name).To(Equal("custom_handler"))
			Expect(otherFuncs[0].Name).To(Equal("random_function"))
		})
	})
	Describe("PrintCreateLanguageStatements", func() {
		plUntrustedHandlerOnly := ddl.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: "testrole", IsPl: true, PlTrusted: false, Handler: 4, Inline: 0, Validator: 0}
		plAllFields := ddl.ProceduralLanguage{Oid: 1, Name: "plperl", Owner: "testrole", IsPl: true, PlTrusted: true, Handler: 1, Inline: 2, Validator: 3}
		plComment := ddl.ProceduralLanguage{Oid: 1, Name: "plpythonu", Owner: "testrole", IsPl: true, PlTrusted: false, Handler: 4, Inline: 0, Validator: 0}
		funcInfoMap := map[uint32]ddl.FunctionInfo{
			1: {QualifiedName: "pg_catalog.plperl_call_handler", Arguments: "", IsInternal: true},
			2: {QualifiedName: "pg_catalog.plperl_inline_handler", Arguments: "internal", IsInternal: true},
			3: {QualifiedName: "pg_catalog.plperl_validator", Arguments: "oid", IsInternal: true},
			4: {QualifiedName: "pg_catalog.plpython_call_handler", Arguments: "", IsInternal: true},
		}
		emptyMetadataMap := ddl.MetadataMap{}

		It("prints untrusted language with a handler only", func() {
			langs := []ddl.ProceduralLanguage{plUntrustedHandlerOnly}

			ddl.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, emptyMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "plpythonu", "PROCEDURAL LANGUAGE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;`)
		})
		It("prints trusted language with handler, inline, and validator", func() {
			langs := []ddl.ProceduralLanguage{plAllFields}

			ddl.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;
ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO testrole;`)
		})
		It("prints multiple create language statements", func() {
			langs := []ddl.ProceduralLanguage{plUntrustedHandlerOnly, plAllFields}

			ddl.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;`, `CREATE TRUSTED PROCEDURAL LANGUAGE plperl HANDLER pg_catalog.plperl_call_handler INLINE pg_catalog.plperl_inline_handler VALIDATOR pg_catalog.plperl_validator;
ALTER FUNCTION pg_catalog.plperl_call_handler() OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_inline_handler(internal) OWNER TO testrole;
ALTER FUNCTION pg_catalog.plperl_validator(oid) OWNER TO testrole;`)
		})
		It("prints a language with privileges, an owner, and a comment", func() {
			langs := []ddl.ProceduralLanguage{plComment}
			langMetadataMap := testutils.DefaultMetadataMap("LANGUAGE", true, true, true)

			ddl.PrintCreateLanguageStatements(backupfile, toc, langs, funcInfoMap, langMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROCEDURAL LANGUAGE plpythonu HANDLER pg_catalog.plpython_call_handler;
ALTER FUNCTION pg_catalog.plpython_call_handler() OWNER TO testrole;

COMMENT ON LANGUAGE plpythonu IS 'This is a language comment.';


ALTER LANGUAGE plpythonu OWNER TO testrole;


REVOKE ALL ON LANGUAGE plpythonu FROM PUBLIC;
REVOKE ALL ON LANGUAGE plpythonu FROM testrole;
GRANT ALL ON LANGUAGE plpythonu TO testrole;`)
		})
	})
	Describe("PrintCreateConversionStatements", func() {
		var (
			convOne     ddl.Conversion
			convTwo     ddl.Conversion
			metadataMap ddl.MetadataMap
		)
		BeforeEach(func() {
			convOne = ddl.Conversion{Oid: 1, Schema: "public", Name: "conv_one", ForEncoding: "UTF8", ToEncoding: "LATIN1", ConversionFunction: "public.converter", IsDefault: false}
			convTwo = ddl.Conversion{Oid: 0, Schema: "public", Name: "conv_two", ForEncoding: "UTF8", ToEncoding: "LATIN1", ConversionFunction: "public.converter", IsDefault: true}
			metadataMap = ddl.MetadataMap{}
		})

		It("prints a non-default conversion", func() {
			conversions := []ddl.Conversion{convOne}
			ddl.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "conv_one", "CONVERSION")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints a default conversion", func() {
			conversions := []ddl.Conversion{convTwo}
			ddl.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE DEFAULT CONVERSION public.conv_two FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints multiple create conversion statements", func() {
			conversions := []ddl.Conversion{convOne, convTwo}
			ddl.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer,
				`CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;`,
				`CREATE DEFAULT CONVERSION public.conv_two FOR 'UTF8' TO 'LATIN1' FROM public.converter;`)
		})
		It("prints a conversion with an owner and a comment", func() {
			conversions := []ddl.Conversion{convOne}
			metadataMap = testutils.DefaultMetadataMap("CONVERSION", false, true, true)
			ddl.PrintCreateConversionStatements(backupfile, toc, conversions, metadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE CONVERSION public.conv_one FOR 'UTF8' TO 'LATIN1' FROM public.converter;

COMMENT ON CONVERSION public.conv_one IS 'This is a conversion comment.';


ALTER CONVERSION public.conv_one OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateForeignDataWrapperStatements", func() {
		funcInfoMap := map[uint32]ddl.FunctionInfo{
			1: {QualifiedName: "pg_catalog.postgresql_fdw_validator", Arguments: "", IsInternal: true},
		}
		It("prints a basic foreign data wrapper", func() {
			foreignDataWrappers := []ddl.ForeignDataWrapper{{Oid: 1, Name: "foreigndata"}}
			ddl.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata;`)
		})
		It("prints a foreign data wrapper with a validator", func() {
			foreignDataWrappers := []ddl.ForeignDataWrapper{{Name: "foreigndata", Validator: 1}}
			ddl.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	VALIDATOR pg_catalog.postgresql_fdw_validator;`)
		})
		It("prints a foreign data wrapper with one option", func() {
			foreignDataWrappers := []ddl.ForeignDataWrapper{{Name: "foreigndata", Options: "debug 'true'"}}
			ddl.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	OPTIONS (debug 'true');`)
		})
		It("prints a foreign data wrapper with two options", func() {
			foreignDataWrappers := []ddl.ForeignDataWrapper{{Name: "foreigndata", Options: "debug 'true', host 'localhost'"}}
			ddl.PrintCreateForeignDataWrapperStatements(backupfile, toc, foreignDataWrappers, funcInfoMap, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreigndata", "FOREIGN DATA WRAPPER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE FOREIGN DATA WRAPPER foreigndata
	OPTIONS (debug 'true', host 'localhost');`)
		})
	})
	Describe("PrintCreateServerStatements", func() {
		It("prints a basic foreign server", func() {
			foreignServers := []ddl.ForeignServer{{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper"}}
			ddl.PrintCreateServerStatements(backupfile, toc, foreignServers, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper;`)
		})
		It("prints a foreign server with one option", func() {
			foreignServers := []ddl.ForeignServer{{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper", Options: "host 'localhost'"}}
			ddl.PrintCreateServerStatements(backupfile, toc, foreignServers, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper
	OPTIONS (host 'localhost');`)
		})
		It("prints a foreign server with two options", func() {
			foreignServers := []ddl.ForeignServer{{Oid: 1, Name: "foreignserver", ForeignDataWrapper: "foreignwrapper", Options: "host 'localhost', dbname 'testdb'"}}
			ddl.PrintCreateServerStatements(backupfile, toc, foreignServers, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	FOREIGN DATA WRAPPER foreignwrapper
	OPTIONS (host 'localhost', dbname 'testdb');`)
		})
		It("prints a foreign server with type and version", func() {
			foreignServers := []ddl.ForeignServer{{Oid: 1, Name: "foreignserver", Type: "server type", Version: "server version", ForeignDataWrapper: "foreignwrapper"}}
			ddl.PrintCreateServerStatements(backupfile, toc, foreignServers, ddl.MetadataMap{})
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "foreignserver", "FOREIGN SERVER")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE SERVER foreignserver
	TYPE 'server type'
	VERSION 'server version'
	FOREIGN DATA WRAPPER foreignwrapper;`)
		})
	})
	Describe("PrintCreateUserMappingStatements", func() {
		It("prints a basic user mapping", func() {
			userMappings := []ddl.UserMapping{{Oid: 1, User: "testrole", Server: "foreignserver"}}
			ddl.PrintCreateUserMappingStatements(backupfile, toc, userMappings)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver;`)
		})
		It("prints a user mapping with one option", func() {
			userMappings := []ddl.UserMapping{{Oid: 1, User: "testrole", Server: "foreignserver", Options: "host 'localhost'"}}
			ddl.PrintCreateUserMappingStatements(backupfile, toc, userMappings)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver
	OPTIONS (host 'localhost');`)
		})
		It("prints a user mapping with two options", func() {
			userMappings := []ddl.UserMapping{{Oid: 1, User: "testrole", Server: "foreignserver", Options: "host 'localhost', dbname 'testdb'"}}
			ddl.PrintCreateUserMappingStatements(backupfile, toc, userMappings)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "testrole ON foreignserver", "USER MAPPING")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE USER MAPPING FOR testrole
	SERVER foreignserver
	OPTIONS (host 'localhost', dbname 'testdb');`)
		})
	})
})
