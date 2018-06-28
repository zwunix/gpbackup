package ddl_test

import (
	"database/sql/driver"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/ddl"
	"github.com/greenplum-db/gpbackup/testutils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/dependencies tests", func() {
	var (
		function1 ddl.Function
		function2 ddl.Function
		function3 ddl.Function
		relation1 ddl.Relation
		relation2 ddl.Relation
		relation3 ddl.Relation
		type1     ddl.Type
		type2     ddl.Type
		type3     ddl.Type
		view1     ddl.View
		view2     ddl.View
		view3     ddl.View
	)

	BeforeEach(func() {
		function1 = ddl.Function{Schema: "public", Name: "function1", Arguments: "integer, integer", DependsUpon: []string{}}
		function2 = ddl.Function{Schema: "public", Name: "function1", Arguments: "numeric, text", DependsUpon: []string{}}
		function3 = ddl.Function{Schema: "public", Name: "function2", Arguments: "integer, integer", DependsUpon: []string{}}
		relation1 = ddl.Relation{Schema: "public", Name: "relation1", DependsUpon: []string{}}
		relation2 = ddl.Relation{Schema: "public", Name: "relation2", DependsUpon: []string{}}
		relation3 = ddl.Relation{Schema: "public", Name: "relation3", DependsUpon: []string{}}
		type1 = ddl.Type{Schema: "public", Name: "type1", DependsUpon: []string{}}
		type2 = ddl.Type{Schema: "public", Name: "type2", DependsUpon: []string{}}
		type3 = ddl.Type{Schema: "public", Name: "type3", DependsUpon: []string{}}
		view1 = ddl.View{Schema: "public", Name: "view1", DependsUpon: []string{}}
		view2 = ddl.View{Schema: "public", Name: "view2", DependsUpon: []string{}}
		view3 = ddl.View{Schema: "public", Name: "view3", DependsUpon: []string{}}
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []ddl.Sortable{relation1, relation2, relation3}

			relations = ddl.TopologicalSort(relations)

			Expect(relations[0].FQN()).To(Equal("public.relation1"))
			Expect(relations[1].FQN()).To(Equal("public.relation2"))
			Expect(relations[2].FQN()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			relation1.DependsUpon = []string{"public.relation3"}
			relations := []ddl.Sortable{relation1, relation2, relation3}

			relations = ddl.TopologicalSort(relations)

			Expect(relations[0].FQN()).To(Equal("public.relation2"))
			Expect(relations[1].FQN()).To(Equal("public.relation3"))
			Expect(relations[2].FQN()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			view1.DependsUpon = []string{"public.view2"}
			view3.DependsUpon = []string{"public.view2"}
			views := []ddl.Sortable{view1, view2, view3}

			views = ddl.TopologicalSort(views)

			Expect(views[0].FQN()).To(Equal("public.view2"))
			Expect(views[1].FQN()).To(Equal("public.view1"))
			Expect(views[2].FQN()).To(Equal("public.view3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			type2.DependsUpon = []string{"public.type1", "public.type3"}
			types := []ddl.Sortable{type1, type2, type3}

			types = ddl.TopologicalSort(types)

			Expect(types[0].FQN()).To(Equal("public.type1"))
			Expect(types[1].FQN()).To(Equal("public.type3"))
			Expect(types[2].FQN()).To(Equal("public.type2"))
		})
		It("sorts the slice correctly if there are complex dependencies", func() {
			type2.DependsUpon = []string{"public.type1", "public.function2(integer, integer)"}
			function3.DependsUpon = []string{"public.type1"}
			sortable := []ddl.Sortable{type1, type2, function3}

			sortable = ddl.TopologicalSort(sortable)

			Expect(sortable[0].FQN()).To(Equal("public.type1"))
			Expect(sortable[1].FQN()).To(Equal("public.function2(integer, integer)"))
			Expect(sortable[2].FQN()).To(Equal("public.type2"))
		})
		It("aborts if dependency loop (this shouldn't be possible)", func() {
			type1.DependsUpon = []string{"public.type3"}
			type2.DependsUpon = []string{"public.type1"}
			type3.DependsUpon = []string{"public.type2"}
			sortable := []ddl.Sortable{type1, type2, type3}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = ddl.TopologicalSort(sortable)
		})
		It("aborts if dependencies are not met", func() {
			type1.DependsUpon = []string{"missing_thing", "public.type2"}
			sortable := []ddl.Sortable{type1, type2}

			defer testhelper.ShouldPanicWithMessage("Dependency resolution failed; see log file gbytes.Buffer for details. This is a bug, please report.")
			sortable = ddl.TopologicalSort(sortable)
		})
	})
	Describe("SortFunctionsAndTypesAndTablesInDependencyOrder", func() {
		It("returns a slice of unsorted functions followed by unsorted types followed by unsorted tables if there are no dependencies among objects", func() {
			functions := []ddl.Function{function1, function2, function3}
			types := []ddl.Type{type1, type2, type3}
			relations := []ddl.Relation{relation1, relation2, relation3}
			results := ddl.SortFunctionsAndTypesAndTablesInDependencyOrder(functions, types, relations)
			expected := []ddl.Sortable{function1, function2, function3, type1, type2, type3, relation1, relation2, relation3}
			Expect(results).To(Equal(expected))
		})
		It("returns a slice of sorted functions, types, and relations if there are dependencies among objects of the same type", func() {
			function2.DependsUpon = []string{"public.function2(integer, integer)"}
			type2.DependsUpon = []string{"public.type3"}
			relation2.DependsUpon = []string{"public.relation3"}
			functions := []ddl.Function{function1, function2, function3}
			types := []ddl.Type{type1, type2, type3}
			relations := []ddl.Relation{relation1, relation2, relation3}
			results := ddl.SortFunctionsAndTypesAndTablesInDependencyOrder(functions, types, relations)
			expected := []ddl.Sortable{function1, function3, type1, type3, relation1, relation3, function2, type2, relation2}
			Expect(results).To(Equal(expected))
		})
		It("returns a slice of sorted functions, types, and relations if there are dependencies among objects of different types", func() {
			function2.DependsUpon = []string{"public.type3"}
			type2.DependsUpon = []string{"public.relation3"}
			relation2.DependsUpon = []string{"public.type1"}
			functions := []ddl.Function{function1, function2, function3}
			types := []ddl.Type{type1, type2, type3}
			relations := []ddl.Relation{relation1, relation2, relation3}
			results := ddl.SortFunctionsAndTypesAndTablesInDependencyOrder(functions, types, relations)
			expected := []ddl.Sortable{function1, function3, type1, type3, relation1, relation3, relation2, function2, type2}
			Expect(results).To(Equal(expected))
		})
	})
	Describe("ConstructFunctionDependencies", func() {
		It("queries function dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			functionRows := sqlmock.NewRows(header).AddRow([]driver.Value{"1", "public.type"}...)

			function1.Oid = 1
			functions := []ddl.Function{function1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(functionRows)
			functions = ddl.ConstructFunctionDependencies(connectionPool, functions)

			Expect(functions[0].DependsUpon).To(Equal([]string{"public.type"}))
		})
		It("queries function dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			header := []string{"oid", "referencedobject"}
			functionRows := sqlmock.NewRows(header).AddRow([]driver.Value{"1", "public.type"}...)

			function1.Oid = 1
			functions := []ddl.Function{function1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(functionRows)
			functions = ddl.ConstructFunctionDependencies(connectionPool, functions)

			Expect(functions[0].DependsUpon).To(Equal([]string{"public.type"}))
		})
	})
	Describe("ConstructBaseTypeDependencies", func() {
		It("queries base type dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			baseTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"2", "public.func(integer, integer)"}...)

			type1.Oid = 2
			type1.Type = "b"
			types := []ddl.Type{type1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(baseTypeRows)
			types = ddl.ConstructBaseTypeDependencies5(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.func(integer, integer)"}))
		})
		It("queries base type dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			funcInfoMap := map[uint32]ddl.FunctionInfo{
				5: {QualifiedName: "public.func", Arguments: "integer, integer"},
			}
			header := []string{"oid", "referencedoid"}
			baseTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"2", "5"}...)

			type1.Oid = 2
			type1.Type = "b"
			types := []ddl.Type{type1}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(baseTypeRows)
			types = ddl.ConstructBaseTypeDependencies4(connectionPool, types, funcInfoMap)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.func(integer, integer)"}))
		})
	})
	Describe("ConstructCompositeTypeDependencies", func() {
		It("queries composite type dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			compTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"3", "public.othertype"}...)

			type2.Oid = 3
			type2.Type = "c"
			types := []ddl.Type{type2}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(compTypeRows)
			types = ddl.ConstructCompositeTypeDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.othertype"}))
		})
		It("queries composite type dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			header := []string{"oid", "referencedobject"}
			compTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"3", "public.othertype"}...)

			type2.Oid = 3
			type2.Type = "c"
			types := []ddl.Type{type2}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(compTypeRows)

			types = ddl.ConstructCompositeTypeDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.othertype"}))
		})
	})
	Describe("ConstructDomainDependencies", func() {
		It("queries domain dependencies in GPDB 5", func() {
			testutils.SetDBVersion(connectionPool, "5.0.0")
			header := []string{"oid", "referencedobject"}
			domainRows := sqlmock.NewRows(header).AddRow([]driver.Value{"4", "public.builtin"}...)

			type3.Oid = 4
			type3.Type = "d"

			types := []ddl.Type{type3}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(domainRows)
			types = ddl.ConstructDomainDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.builtin"}))
		})
		It("queries domain dependencies in GPDB 4.3", func() {
			testutils.SetDBVersion(connectionPool, "4.3.0")
			header := []string{"oid", "referencedobject"}
			domainRows := sqlmock.NewRows(header).AddRow([]driver.Value{"4", "public.builtin"}...)

			type3.Oid = 4
			type3.Type = "d"
			types := []ddl.Type{type3}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(domainRows)
			types = ddl.ConstructDomainDependencies(connectionPool, types)

			Expect(types[0].DependsUpon).To(Equal([]string{"public.builtin"}))
		})
	})
	Describe("ConstructFunctionAndTypeAndTableMetadataMap", func() {
		It("composes metadata maps for functions, types, and tables into one map", func() {
			funcMap := ddl.MetadataMap{1: ddl.ObjectMetadata{Comment: "function"}}
			typeMap := ddl.MetadataMap{2: ddl.ObjectMetadata{Comment: "type"}}
			tableMap := ddl.MetadataMap{3: ddl.ObjectMetadata{Comment: "relation"}}
			result := ddl.ConstructFunctionAndTypeAndTableMetadataMap(funcMap, typeMap, tableMap)
			expected := ddl.MetadataMap{
				1: ddl.ObjectMetadata{Comment: "function"},
				2: ddl.ObjectMetadata{Comment: "type"},
				3: ddl.ObjectMetadata{Comment: "relation"},
			}
			Expect(result).To(Equal(expected))
		})
	})
	Describe("SortViews", func() {
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			view1.DependsUpon = []string{"public.view2"}
			view3.DependsUpon = []string{"public.view2"}
			views := []ddl.View{view1, view2, view3}

			views = ddl.SortViews(views)

			Expect(views[0].FQN()).To(Equal("public.view2"))
			Expect(views[1].FQN()).To(Equal("public.view1"))
			Expect(views[2].FQN()).To(Equal("public.view3"))
		})
	})
})
