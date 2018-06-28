package ddl_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/ddl"
	"github.com/greenplum-db/gpbackup/testutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_externals tests", func() {
	testTable := ddl.Relation{Schema: "public", Name: "tablename"}

	distRandom := "DISTRIBUTED RANDOMLY"

	heapOpts := ""

	partDefEmpty := ""
	partTemplateDefEmpty := ""
	colDefsEmpty := []ddl.ColumnDefinition{}
	extTableEmpty := ddl.ExternalTableDefinition{Oid: 0, Type: -2, Protocol: -2, ExecLocation: "ALL_SEGMENTS", FormatType: "t", RejectLimit: 0, Encoding: "UTF-8", Writable: false, URIs: nil}

	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("DetermineExternalTableCharacteristics", func() {
		var extTableDef ddl.ExternalTableDefinition
		BeforeEach(func() {
			extTableDef = extTableEmpty
		})
		Context("Type classification", func() {
			It("classifies a READABLE EXTERNAL table correctly", func() {
				extTableDef.Location = "file://host:port/path/file"
				typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(ddl.READABLE))
				Expect(proto).To(Equal(ddl.FILE))
			})
			It("classifies a WRITABLE EXTERNAL table correctly", func() {
				extTableDef.Location = "file://host:port/path/file"
				extTableDef.Writable = true
				typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(ddl.WRITABLE))
				Expect(proto).To(Equal(ddl.FILE))
			})
			It("classifies a READABLE EXTERNAL WEB table with a LOCATION correctly", func() {
				extTableDef.Location = "http://webhost:port/path/file"
				typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(ddl.READABLE_WEB))
				Expect(proto).To(Equal(ddl.HTTP))
			})
			It("classifies a WRITABLE EXTERNAL WEB table with a LOCATION correctly", func() {
				extTableDef.Location = "http://webhost:port/path/file"
				extTableDef.Writable = true
				typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(ddl.WRITABLE_WEB))
				Expect(proto).To(Equal(ddl.HTTP))
			})
			It("classifies a READABLE EXTERNAL WEB table with an EXECUTE correctly", func() {
				extTableDef.Command = "hostname"
				typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(ddl.READABLE_WEB))
				Expect(proto).To(Equal(ddl.HTTP))
			})
			It("classifies a WRITABLE EXTERNAL WEB table correctly", func() {
				extTableDef.Command = "hostname"
				extTableDef.Writable = true
				typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
				Expect(typ).To(Equal(ddl.WRITABLE_WEB))
				Expect(proto).To(Equal(ddl.HTTP))
			})
		})
		DescribeTable("Protocol classification", func(location string, expectedType int, expectedProto int) {
			extTableDef := extTableEmpty
			extTableDef.Location = location
			typ, proto := ddl.DetermineExternalTableCharacteristics(extTableDef)
			Expect(typ).To(Equal(expectedType))
			Expect(proto).To(Equal(expectedProto))
		},
			Entry("classifies file:// locations correctly", "file://host:port/path/file", ddl.READABLE, ddl.FILE),
			Entry("classifies gpfdist:// locations correctly", "gpfdist://host:port/file_pattern", ddl.READABLE, ddl.GPFDIST),
			Entry("classifies gpfdists:// locations correctly", "gpfdists://host:port/file_pattern", ddl.READABLE, ddl.GPFDIST),
			Entry("classifies gphdfs:// locations correctly", "gphdfs://host:port/path/file", ddl.READABLE, ddl.GPHDFS),
			Entry("classifies http:// locations correctly", "http://webhost:port/path/file", ddl.READABLE_WEB, ddl.HTTP),
			Entry("classifies https:// locations correctly", "https://webhost:port/path/file", ddl.READABLE_WEB, ddl.HTTP),
			Entry("classifies s3:// locations correctly", "s3://s3_endpoint:port/bucket_name/s3_prefix", ddl.READABLE, ddl.S3),
		)
	})
	Describe("PrintExternalTableCreateStatement", func() {
		var tableDef ddl.TableDefinition
		var extTableDef ddl.ExternalTableDefinition
		BeforeEach(func() {
			tableDef = ddl.TableDefinition{DistPolicy: distRandom, PartDef: partDefEmpty, PartTemplateDef: partTemplateDefEmpty, StorageOpts: heapOpts, TablespaceName: "", ColumnDefs: colDefsEmpty, IsExternal: true, ExtTableDef: extTableEmpty}
			extTableDef = extTableEmpty
		})

		It("prints a CREATE block for a READABLE EXTERNAL table", func() {
			extTableDef.Location = "file://host:port/path/file"
			extTableDef.URIs = []string{"file://host:port/path/file"}
			tableDef.ExtTableDef = extTableDef
			ddl.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)
			testutils.ExpectEntry(toc.PredataEntries, 0, "public", "", "tablename", "TABLE")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE READABLE EXTERNAL TABLE public.tablename (
) LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8';`)
		})
		It("prints a CREATE block for a WRITABLE EXTERNAL table", func() {
			extTableDef.Location = "file://host:port/path/file"
			extTableDef.URIs = []string{"file://host:port/path/file"}
			extTableDef.Writable = true
			tableDef.ExtTableDef = extTableDef
			ddl.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE WRITABLE EXTERNAL TABLE public.tablename (
) LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8'
DISTRIBUTED RANDOMLY;`)
		})
		It("prints a CREATE block for a READABLE EXTERNAL WEB table with a LOCATION", func() {
			extTableDef.Location = "http://webhost:port/path/file"
			extTableDef.URIs = []string{"http://webhost:port/path/file"}
			tableDef.ExtTableDef = extTableDef
			ddl.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) LOCATION (
	'http://webhost:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8';`)
		})
		It("prints a CREATE block for a READABLE EXTERNAL WEB table with an EXECUTE", func() {
			extTableDef.Command = "hostname"
			tableDef.ExtTableDef = extTableDef
			ddl.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE READABLE EXTERNAL WEB TABLE public.tablename (
) EXECUTE 'hostname'
FORMAT 'text'
ENCODING 'UTF-8';`)
		})
		It("prints a CREATE block for a WRITABLE EXTERNAL WEB table", func() {
			extTableDef.Command = "hostname"
			extTableDef.Writable = true
			tableDef.ExtTableDef = extTableDef
			ddl.PrintExternalTableCreateStatement(backupfile, toc, testTable, tableDef)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE WRITABLE EXTERNAL WEB TABLE public.tablename (
) EXECUTE 'hostname'
FORMAT 'text'
ENCODING 'UTF-8'
DISTRIBUTED RANDOMLY;`)
		})
	})
	Describe("PrintExternalTableStatements", func() {
		var extTableDef ddl.ExternalTableDefinition
		BeforeEach(func() {
			extTableDef = extTableEmpty
			extTableDef.Type = ddl.READABLE
			extTableDef.Protocol = ddl.FILE
		})

		Context("FORMAT options", func() {
			BeforeEach(func() {
				extTableDef.Location = "file://host:port/path/file"
				extTableDef.URIs = []string{"file://host:port/path/file"}
			})
			It("prints a CREATE block for a table in Avro format, no options provided", func() {
				extTableDef.FormatType = "a"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'avro'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table in Parquet format, no options provided", func() {
				extTableDef.FormatType = "p"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'parquet'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table in CSV format, some options provided", func() {
				extTableDef.FormatType = "c"
				extTableDef.FormatOpts = `delimiter ',' null '' escape '"' quote '"'`
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'csv' (delimiter ',' null '' escape '"' quote '"')
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table in text format, some options provided", func() {
				extTableDef.FormatType = "t"
				extTableDef.FormatOpts = `delimiter '  ' null '\N' escape '\'`
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text' (delimiter '  ' null '\N' escape '\')
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table in custom format, formatter provided", func() {
				extTableDef.FormatType = "b"
				extTableDef.FormatOpts = `formatter gphdfs_import other_opt 'foo'`
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'custom' (formatter = gphdfs_import, other_opt = 'foo')
ENCODING 'UTF-8'`)
			})
		})
		Context("EXECUTE options", func() {
			BeforeEach(func() {
				extTableDef = extTableEmpty
				extTableDef.Type = ddl.READABLE_WEB
				extTableDef.Protocol = ddl.HTTP
				extTableDef.Command = "hostname"
				extTableDef.FormatType = "t"
			})

			It("prints a CREATE block for a table with EXECUTE ON ALL", func() {
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname'
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON MASTER", func() {
				extTableDef.ExecLocation = "MASTER_ONLY"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON MASTER
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON [number]", func() {
				extTableDef.ExecLocation = "TOTAL_SEGS:3"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON 3
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON HOST", func() {
				extTableDef.ExecLocation = "PER_HOST"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON HOST
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON HOST [host]", func() {
				extTableDef.ExecLocation = "HOST:localhost"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON HOST 'localhost'
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with EXECUTE ON SEGMENT [segid]", func() {
				extTableDef.ExecLocation = "SEGMENT_ID:0"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'hostname' ON SEGMENT 0
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table with single quotes in its EXECUTE clause", func() {
				extTableDef.Command = "fake'command"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `EXECUTE 'fake''command'
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
		})
		Context("Miscellaneous options", func() {
			BeforeEach(func() {
				extTableDef = extTableEmpty
				extTableDef.Type = ddl.READABLE
				extTableDef.Protocol = ddl.FILE
				extTableDef.Location = "file://host:port/path/file"
				extTableDef.URIs = []string{"file://host:port/path/file"}
			})

			It("prints a CREATE block for an S3 table with ON MASTER", func() {
				extTableDef.Protocol = ddl.S3
				extTableDef.Location = "s3://s3_endpoint:port/bucket_name/s3_prefix"
				extTableDef.URIs = []string{"s3://s3_endpoint:port/bucket_name/s3_prefix"}
				extTableDef.ExecLocation = "MASTER_ONLY"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	's3://s3_endpoint:port/bucket_name/s3_prefix'
) ON MASTER
FORMAT 'text'
ENCODING 'UTF-8'`)
			})
			It("prints a CREATE block for a table using error logging with an error table", func() {
				extTableDef.ErrTableName = "error_table"
				extTableDef.ErrTableSchema = "error_table_schema"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8'
LOG ERRORS INTO error_table_schema.error_table`)
			})
			It("prints a CREATE block for a table using error logging without an error table", func() {
				extTableDef.ErrTableName = "tablename"
				extTableDef.ErrTableSchema = "public"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8'
LOG ERRORS`)
			})
			It("prints a CREATE block for a table with a row-based reject limit", func() {
				extTableDef.RejectLimit = 2
				extTableDef.RejectLimitType = "r"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8'
SEGMENT REJECT LIMIT 2 ROWS`)
			})
			It("prints a CREATE block for a table with a percent-based reject limit", func() {
				extTableDef.RejectLimit = 2
				extTableDef.RejectLimitType = "p"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8'
SEGMENT REJECT LIMIT 2 PERCENT`)
			})
			It("prints a CREATE block for a table with error logging and a row-based reject limit", func() {
				extTableDef.ErrTableName = "tablename"
				extTableDef.ErrTableSchema = "public"
				extTableDef.RejectLimit = 2
				extTableDef.RejectLimitType = "r"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
ENCODING 'UTF-8'
LOG ERRORS
SEGMENT REJECT LIMIT 2 ROWS`)
			})
			It("prints a CREATE block for a table with custom options", func() {
				extTableDef.Options = "foo 'bar'\n\tbar 'baz'"
				ddl.PrintExternalTableStatements(backupfile, testTable, extTableDef)
				testhelper.ExpectRegexp(buffer, `LOCATION (
	'file://host:port/path/file'
)
FORMAT 'text'
OPTIONS (
	foo 'bar'
	bar 'baz'
)
ENCODING 'UTF-8'`)
			})
		})
	})
	Describe("PrintExternalProtocolStatements", func() {
		protocolUntrustedReadWrite := ddl.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: 1, WriteFunction: 2, Validator: 0}
		protocolUntrustedReadValidator := ddl.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: 1, WriteFunction: 0, Validator: 3}
		protocolUntrustedWriteOnly := ddl.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: false, ReadFunction: 0, WriteFunction: 2, Validator: 0}
		protocolTrustedReadWriteValidator := ddl.ExternalProtocol{Oid: 1, Name: "s3", Owner: "testrole", Trusted: true, ReadFunction: 1, WriteFunction: 2, Validator: 3}
		protocolUntrustedReadOnly := ddl.ExternalProtocol{Oid: 1, Name: "s4", Owner: "testrole", Trusted: false, ReadFunction: 4, WriteFunction: 0, Validator: 0}
		protocolInternal := ddl.ExternalProtocol{Oid: 1, Name: "gphdfs", Owner: "testrole", Trusted: false, ReadFunction: 5, WriteFunction: 6, Validator: 7}
		protocolInternalReadWrite := ddl.ExternalProtocol{Oid: 1, Name: "gphdfs", Owner: "testrole", Trusted: false, ReadFunction: 5, WriteFunction: 6, Validator: 0}
		funcInfoMap := map[uint32]ddl.FunctionInfo{
			1: {QualifiedName: "public.read_fn_s3", Arguments: ""},
			2: {QualifiedName: "public.write_fn_s3", Arguments: ""},
			3: {QualifiedName: "public.validator", Arguments: ""},
			4: {QualifiedName: "public.read_fn_s4", Arguments: ""},
			5: {QualifiedName: "pg_catalog.read_internal_fn", Arguments: "", IsInternal: true},
			6: {QualifiedName: "pg_catalog.write_internal_fn", Arguments: "", IsInternal: true},
			7: {QualifiedName: "pg_catalog.validate_internal_fn", Arguments: "", IsInternal: true},
		}
		emptyMetadataMap := ddl.MetadataMap{}

		It("prints untrusted protocol with read and write function", func() {
			protos := []ddl.ExternalProtocol{protocolUntrustedReadWrite}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testutils.ExpectEntry(toc.PredataEntries, 0, "", "", "s3", "PROTOCOL")
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3);`)
		})
		It("prints untrusted protocol with read and validator", func() {
			protos := []ddl.ExternalProtocol{protocolUntrustedReadValidator}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, validatorfunc = public.validator);`)
		})
		It("prints untrusted protocol with write function only", func() {
			protos := []ddl.ExternalProtocol{protocolUntrustedWriteOnly}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s3 (writefunc = public.write_fn_s3);`)
		})
		It("prints trusted protocol with read, write, and validator", func() {
			protos := []ddl.ExternalProtocol{protocolTrustedReadWriteValidator}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE TRUSTED PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3, validatorfunc = public.validator);`)
		})
		It("prints multiple protocols", func() {
			protos := []ddl.ExternalProtocol{protocolUntrustedWriteOnly, protocolUntrustedReadOnly}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s3 (writefunc = public.write_fn_s3);`, `CREATE PROTOCOL s4 (readfunc = public.read_fn_s4);`)
		})
		It("skips printing protocols where all functions are internal", func() {
			protos := []ddl.ExternalProtocol{protocolInternal, protocolUntrustedReadOnly}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testhelper.NotExpectRegexp(buffer, `CREATE PROTOCOL gphdfs`)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s4 (readfunc = public.read_fn_s4);`)
		})
		It("skips printing protocols without validator where all functions are internal", func() {
			protos := []ddl.ExternalProtocol{protocolInternalReadWrite, protocolUntrustedReadOnly}

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, emptyMetadataMap)
			testhelper.NotExpectRegexp(buffer, `CREATE PROTOCOL gphdfs`)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s4 (readfunc = public.read_fn_s4);`)
		})
		It("prints a protocol with privileges and an owner", func() {
			protos := []ddl.ExternalProtocol{protocolUntrustedReadWrite}
			protoMetadataMap := testutils.DefaultMetadataMap("PROTOCOL", true, true, false)

			ddl.PrintCreateExternalProtocolStatements(backupfile, toc, protos, funcInfoMap, protoMetadataMap)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `CREATE PROTOCOL s3 (readfunc = public.read_fn_s3, writefunc = public.write_fn_s3);


ALTER PROTOCOL s3 OWNER TO testrole;


REVOKE ALL ON PROTOCOL s3 FROM PUBLIC;
REVOKE ALL ON PROTOCOL s3 FROM testrole;
GRANT ALL ON PROTOCOL s3 TO testrole;`)
		})
	})
	Describe("PrintExchangeExternalPartitionStatements", func() {
		tables := []ddl.Relation{
			{Oid: 1, Schema: "public", Name: "partition_table_ext_part_"},
			{Oid: 2, Schema: "public", Name: "partition_table"},
		}
		emptyPartInfoMap := make(map[uint32]ddl.PartitionInfo, 0)
		It("writes an alter statement for a named partition", func() {
			externalPartition := ddl.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "partition_name",
				PartitionRank:          0,
				IsExternal:             true,
			}
			externalPartitions := []ddl.PartitionInfo{externalPartition}
			ddl.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, emptyPartInfoMap, tables)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.partition_table EXCHANGE PARTITION partition_name WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement using rank for an unnamed partition", func() {
			externalPartition := ddl.PartitionInfo{
				PartitionRuleOid:       1,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitions := []ddl.PartitionInfo{externalPartition}
			ddl.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, emptyPartInfoMap, tables)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.partition_table EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement using rank for a two level partition", func() {
			externalPartition := ddl.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitionParent := ddl.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          3,
				IsExternal:             false,
			}
			partInfoMap := map[uint32]ddl.PartitionInfo{externalPartitionParent.PartitionRuleOid: externalPartitionParent}
			externalPartitions := []ddl.PartitionInfo{externalPartition}
			ddl.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, partInfoMap, tables)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.partition_table ALTER PARTITION FOR (RANK(3)) EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement using partition name for a two level partition", func() {
			externalPartition := ddl.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitionParent := ddl.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            3,
				PartitionName:          "partition_name",
				PartitionRank:          0,
				IsExternal:             false,
			}
			partInfoMap := map[uint32]ddl.PartitionInfo{externalPartitionParent.PartitionRuleOid: externalPartitionParent}
			externalPartitions := []ddl.PartitionInfo{externalPartition}
			ddl.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, partInfoMap, tables)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.partition_table ALTER PARTITION partition_name EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
		It("writes an alter statement for a three level partition", func() {
			externalPartition := ddl.PartitionInfo{
				PartitionRuleOid:       10,
				PartitionParentRuleOid: 11,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            1,
				PartitionName:          "",
				PartitionRank:          1,
				IsExternal:             true,
			}
			externalPartitionParent1 := ddl.PartitionInfo{
				PartitionRuleOid:       11,
				PartitionParentRuleOid: 12,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            0,
				PartitionName:          "partition_name",
				PartitionRank:          0,
				IsExternal:             false,
			}
			externalPartitionParent2 := ddl.PartitionInfo{
				PartitionRuleOid:       12,
				PartitionParentRuleOid: 0,
				ParentRelationOid:      2,
				ParentSchema:           "public",
				ParentRelationName:     "partition_table",
				RelationOid:            0,
				PartitionName:          "",
				PartitionRank:          3,
				IsExternal:             false,
			}
			partInfoMap := map[uint32]ddl.PartitionInfo{externalPartitionParent1.PartitionRuleOid: externalPartitionParent1, externalPartitionParent2.PartitionRuleOid: externalPartitionParent2}
			externalPartitions := []ddl.PartitionInfo{externalPartition}
			ddl.PrintExchangeExternalPartitionStatements(backupfile, toc, externalPartitions, partInfoMap, tables)
			testutils.AssertBufferContents(toc.PredataEntries, buffer, `ALTER TABLE public.partition_table ALTER PARTITION FOR (RANK(3)) ALTER PARTITION partition_name EXCHANGE PARTITION FOR (RANK(1)) WITH TABLE public.partition_table_ext_part_ WITHOUT VALIDATION;

DROP TABLE public.partition_table_ext_part_;`)
		})
	})
})
