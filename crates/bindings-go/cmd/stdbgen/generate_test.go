package main

import (
	"strings"
	"testing"
)

// ── generateWithTemplate error paths ─────────────────────────────────────────

func TestGenerateWithTemplate_BadTemplate(t *testing.T) {
	schema := Schema{Package: "testpkg"}
	_, err := generateWithTemplate(schema, "test.yaml", "{{.InvalidSyntax")
	if err == nil {
		t.Fatal("expected template parse error")
	}
	if !strings.Contains(err.Error(), "template parse error") {
		t.Errorf("expected 'template parse error', got %q", err.Error())
	}
}

func TestGenerateWithTemplate_ExecuteError(t *testing.T) {
	schema := Schema{Package: "testpkg"}
	// Template references a method that doesn't exist on the data
	_, err := generateWithTemplate(schema, "test.yaml", "{{.NonExistentMethod 42}}")
	if err == nil {
		t.Fatal("expected template execute error")
	}
	if !strings.Contains(err.Error(), "template execute error") {
		t.Errorf("expected 'template execute error', got %q", err.Error())
	}
}

func TestGenerateWithTemplate_GofmtError(t *testing.T) {
	schema := Schema{Package: "testpkg"}
	// Template produces output that is syntactically valid template but not valid Go
	_, err := generateWithTemplate(schema, "test.yaml", "this is not valid go code !!!")
	if err == nil {
		t.Fatal("expected gofmt error")
	}
	if !strings.Contains(err.Error(), "gofmt error") {
		t.Errorf("expected 'gofmt error', got %q", err.Error())
	}
}

// ── generateTests ────────────────────────────────────────────────────────────

func TestGenerateTests_WithTables(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "Player",
				Columns: []Column{
					{Name: "id", Type: "U64"},
					{Name: "name", Type: "String"},
					{Name: "active", Type: "Bool"},
					{Name: "score", Type: "F32"},
					{Name: "data", Type: "Bytes"},
					{Name: "identity", Type: "Identity"},
					{Name: "created_at", Type: "Timestamp"},
					{Name: "nickname", Type: "Option<String>"},
				},
			},
		},
	}
	out, err := generateTests(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generateTests failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "package testpkg") {
		t.Error("test file should have package testpkg")
	}
	if !strings.Contains(src, "TestPlayerEncodeDecode") {
		t.Error("should generate TestPlayerEncodeDecode")
	}
	if !strings.Contains(src, "encodePlayer") {
		t.Error("should call encodePlayer")
	}
	if !strings.Contains(src, "decodePlayer") {
		t.Error("should call decodePlayer")
	}
	if !strings.Contains(src, "//go:build tinygo") {
		t.Error("should have tinygo build constraint")
	}
}

func TestGenerateTests_NoTables(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
	}
	out, err := generateTests(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generateTests failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "package testpkg") {
		t.Error("test file should have package testpkg")
	}
}

func TestGenerateTests_AllColumnTypes(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "AllTypes",
				Columns: []Column{
					{Name: "s", Type: "String"},
					{Name: "b", Type: "Bool"},
					{Name: "i8", Type: "I8"},
					{Name: "u8", Type: "U8"},
					{Name: "i16", Type: "I16"},
					{Name: "u16", Type: "U16"},
					{Name: "i32", Type: "I32"},
					{Name: "u32", Type: "U32"},
					{Name: "i64", Type: "I64"},
					{Name: "u64", Type: "U64"},
					{Name: "f32", Type: "F32"},
					{Name: "f64", Type: "F64"},
					{Name: "bytes", Type: "Bytes"},
					{Name: "id", Type: "Identity"},
					{Name: "ts", Type: "Timestamp"},
					{Name: "opt_u32", Type: "Option<U32>"},
				},
			},
		},
	}
	_, err := generateTests(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generateTests with all types failed: %v", err)
	}
}

// ── buildFuncMap ─────────────────────────────────────────────────────────────

func TestBuildFuncMap(t *testing.T) {
	tc := map[string][]Column{
		"User": {{Name: "id", Type: "U64"}},
	}
	fm := buildFuncMap(tc)

	// Verify all expected keys are present.
	expectedKeys := []string{
		"title", "camelTitle", "camelCols", "lower",
		"tableAccess", "reducerVisibility",
		"algebraicType", "goType", "readMethod", "writeMethod", "specialWrite",
		"join", "encodeCol", "add",
		"customAlgebraicType", "pkColIDs", "colIDs",
		"idxKeyGoType", "idxKeyWrite",
	}
	for _, key := range expectedKeys {
		if fm[key] == nil {
			t.Errorf("buildFuncMap missing key %q", key)
		}
	}

	// Test "add" function
	addFn := fm["add"].(func(int, int) int)
	if got := addFn(3, 4); got != 7 {
		t.Errorf("add(3, 4) = %d, want 7", got)
	}

	// Test "colIDs" with known columns
	colIDsFn := fm["colIDs"].(func(string, []string) string)
	if got := colIDsFn("User", []string{"id"}); got != "0" {
		t.Errorf("colIDs(\"User\", [\"id\"]) = %q, want \"0\"", got)
	}

	// Test "colIDs" with missing column (should fallback to 0)
	got := colIDsFn("User", []string{"missing"})
	if got != "0" {
		t.Errorf("colIDs(\"User\", [\"missing\"]) = %q, want \"0\"", got)
	}

	// Test "idxKeyGoType"
	idxGoTypeFn := fm["idxKeyGoType"].(func(string, []string) string)
	if got := idxGoTypeFn("User", []string{"id"}); got != "uint64" {
		t.Errorf("idxKeyGoType(\"User\", [\"id\"]) = %q, want \"uint64\"", got)
	}

	// Test "idxKeyWrite"
	idxWriteFn := fm["idxKeyWrite"].(func(string, []string) string)
	got = idxWriteFn("User", []string{"id"})
	if !strings.Contains(got, "w.WriteU64") {
		t.Errorf("idxKeyWrite(\"User\", [\"id\"]) = %q, want to contain w.WriteU64", got)
	}

	// Test "pkColIDs"
	pkColIDsFn := fm["pkColIDs"].(func(string, []string, []Column) string)
	cols := []Column{{Name: "id", Type: "U64"}, {Name: "name", Type: "String"}}
	if got := pkColIDsFn("User", []string{"name"}, cols); got != "1" {
		t.Errorf("pkColIDs(\"User\", [\"name\"], cols) = %q, want \"1\"", got)
	}
}

// ── buildPrefixFilters ──────────────────────────────────────────────────────

func TestBuildPrefixFilters_NoBTreeIndexes(t *testing.T) {
	schema := Schema{
		Tables: []Table{
			{Name: "User", Columns: []Column{{Name: "id", Type: "U64"}}},
		},
	}
	tc := map[string][]Column{"User": schema.Tables[0].Columns}
	filters := buildPrefixFilters(schema, tc)
	if len(filters) != 0 {
		t.Errorf("expected no prefix filters, got %d", len(filters))
	}
}

func TestBuildPrefixFilters_SingleColumnBTree(t *testing.T) {
	schema := Schema{
		Tables: []Table{
			{
				Name:    "User",
				Columns: []Column{{Name: "id", Type: "U64"}},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_id", Columns: []string{"id"}},
				},
			},
		},
	}
	tc := map[string][]Column{"User": schema.Tables[0].Columns}
	filters := buildPrefixFilters(schema, tc)
	if len(filters) != 0 {
		t.Errorf("single-column BTree should not generate prefix filters, got %d", len(filters))
	}
}

func TestBuildPrefixFilters_MultiColumnBTree(t *testing.T) {
	schema := Schema{
		Tables: []Table{
			{
				Name: "Message",
				Columns: []Column{
					{Name: "sender", Type: "Identity"},
					{Name: "sent_at", Type: "Timestamp"},
					{Name: "text", Type: "String"},
				},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_sender_sent", Columns: []string{"sender", "sent_at", "text"}},
				},
			},
		},
	}
	tc := map[string][]Column{"Message": schema.Tables[0].Columns}
	filters := buildPrefixFilters(schema, tc)

	// 3 columns → prefix lengths 1 and 2 → 2 prefix filters
	if len(filters) != 2 {
		t.Fatalf("expected 2 prefix filters, got %d", len(filters))
	}

	// First filter: prefix=[sender], trailing=sent_at
	f0 := filters[0]
	if f0.FuncName != "FilterMessageBySenderAndSentAtRange" {
		t.Errorf("filter[0].FuncName = %q, want \"FilterMessageBySenderAndSentAtRange\"", f0.FuncName)
	}
	if f0.TableName != "Message" {
		t.Errorf("filter[0].TableName = %q, want \"Message\"", f0.TableName)
	}
	if f0.NumPrefix != 1 {
		t.Errorf("filter[0].NumPrefix = %d, want 1", f0.NumPrefix)
	}
	if len(f0.PrefixCols) != 1 {
		t.Errorf("filter[0] should have 1 prefix col, got %d", len(f0.PrefixCols))
	}
	if f0.PrefixCols[0].GoType != "types.Identity" {
		t.Errorf("filter[0].PrefixCols[0].GoType = %q, want \"types.Identity\"", f0.PrefixCols[0].GoType)
	}
	if !strings.Contains(f0.PrefixCols[0].EncodeStmt, ".WriteBsatn(w)") {
		t.Errorf("filter[0] sender encode should use WriteBsatn, got %q", f0.PrefixCols[0].EncodeStmt)
	}
	if f0.TrailingType != "types.Timestamp" {
		t.Errorf("filter[0].TrailingType = %q, want \"types.Timestamp\"", f0.TrailingType)
	}

	// Second filter: prefix=[sender, sent_at], trailing=text
	f1 := filters[1]
	if f1.FuncName != "FilterMessageBySenderSentAtAndTextRange" {
		t.Errorf("filter[1].FuncName = %q, want \"FilterMessageBySenderSentAtAndTextRange\"", f1.FuncName)
	}
	if f1.NumPrefix != 2 {
		t.Errorf("filter[1].NumPrefix = %d, want 2", f1.NumPrefix)
	}
	if len(f1.PrefixCols) != 2 {
		t.Errorf("filter[1] should have 2 prefix cols, got %d", len(f1.PrefixCols))
	}
}

func TestBuildPrefixFilters_StandardType(t *testing.T) {
	schema := Schema{
		Tables: []Table{
			{
				Name: "Score",
				Columns: []Column{
					{Name: "player_id", Type: "U64"},
					{Name: "level", Type: "U32"},
				},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_player_level", Columns: []string{"player_id", "level"}},
				},
			},
		},
	}
	tc := map[string][]Column{"Score": schema.Tables[0].Columns}
	filters := buildPrefixFilters(schema, tc)

	if len(filters) != 1 {
		t.Fatalf("expected 1 prefix filter, got %d", len(filters))
	}

	f := filters[0]
	if !strings.Contains(f.PrefixCols[0].EncodeStmt, "w.WriteU64") {
		t.Errorf("standard type should use writeMethod, got %q", f.PrefixCols[0].EncodeStmt)
	}
}

// ── generate (end-to-end) ───────────────────────────────────────────────────

func TestGenerate_MinimalSchema(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "package testpkg") {
		t.Error("output should contain package declaration")
	}
	if !strings.Contains(src, "Code generated by stdbgen") {
		t.Error("output should contain generated header")
	}
}

func TestGenerate_TableWithColumns(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "User",
				Columns: []Column{
					{Name: "id", Type: "U64"},
					{Name: "name", Type: "String"},
					{Name: "active", Type: "Bool"},
				},
				PrimaryKey: []string{"id"},
				Access:     "public",
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)

	// Check struct generation
	if !strings.Contains(src, "type User struct") {
		t.Error("should generate User struct")
	}
	if !strings.Contains(src, "Id") || !strings.Contains(src, "uint64") {
		t.Error("should generate Id uint64 field")
	}
	if !strings.Contains(src, "Name") {
		t.Error("should generate Name field")
	}
	if !strings.Contains(src, "Active") || !strings.Contains(src, "bool") {
		t.Error("should generate Active bool field")
	}

	// Check encode/decode functions
	if !strings.Contains(src, "func encodeUser(") {
		t.Error("should generate encodeUser function")
	}
	if !strings.Contains(src, "func decodeUser(") {
		t.Error("should generate decodeUser function")
	}

	// Check table registration
	if !strings.Contains(src, `Name: "User"`) {
		t.Error("should register table with name User")
	}
	if !strings.Contains(src, "spacetimedb.TableAccessPublic") {
		t.Error("should set public access")
	}
}

func TestGenerate_PrivateTable(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name:    "Secret",
				Columns: []Column{{Name: "id", Type: "U32"}},
				Access:  "private",
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if !strings.Contains(string(out), "spacetimedb.TableAccessPrivate") {
		t.Error("should set private access")
	}
}

func TestGenerate_EventTable(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name:    "LogEntry",
				Columns: []Column{{Name: "msg", Type: "String"}},
				IsEvent: true,
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if !strings.Contains(string(out), "IsEvent: true") {
		t.Error("should set IsEvent true")
	}
}

func TestGenerate_UniqueIndex(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "User",
				Columns: []Column{
					{Name: "id", Type: "U64"},
					{Name: "email", Type: "String"},
				},
				UniqueIndexes: []UniqueIndex{
					{Name: "idx_email", Columns: []string{"email"}},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "FindUserByEmail") {
		t.Error("should generate FindUserByEmail")
	}
	if !strings.Contains(src, "DeleteUserByEmail") {
		t.Error("should generate DeleteUserByEmail")
	}
	if !strings.Contains(src, "UpdateUserByEmail") {
		t.Error("should generate UpdateUserByEmail")
	}
	if !strings.Contains(src, "NewUniqueIndex") {
		t.Error("should create unique index")
	}
}

func TestGenerate_BTreeIndex(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "Score",
				Columns: []Column{
					{Name: "player_id", Type: "U64"},
					{Name: "value", Type: "I32"},
				},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_value", Columns: []string{"value"}},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "FilterScoreByValue") {
		t.Error("should generate FilterScoreByValue")
	}
	if !strings.Contains(src, "FilterScoreByValueRange") {
		t.Error("should generate FilterScoreByValueRange")
	}
	if !strings.Contains(src, "NewBTreeIndex") {
		t.Error("should create BTree index")
	}
}

func TestGenerate_MultiColumnBTreeIndex(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "Event",
				Columns: []Column{
					{Name: "category", Type: "U32"},
					{Name: "timestamp", Type: "U64"},
				},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_cat_ts", Columns: []string{"category", "timestamp"}},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "FilterEventByCategoryAndTimestampRange") {
		t.Error("should generate prefix filter function")
	}
	if !strings.Contains(src, "prefixFilterWriter") {
		t.Error("should reference prefixFilterWriter")
	}
}

func TestGenerate_Reducer(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Reducers: []Reducer{
			{
				Name: "AddUser",
				Params: []Column{
					{Name: "name", Type: "String"},
					{Name: "age", Type: "U32"},
				},
				Visibility: "public",
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, `Name: "AddUser"`) {
		t.Error("should register reducer AddUser")
	}
	if !strings.Contains(src, "handleAddUser") {
		t.Error("should generate handleAddUser")
	}
	if !strings.Contains(src, "ReducerVisibilityClientCallable") {
		t.Error("should set public visibility")
	}
	if !strings.Contains(src, "ReadBytesSourceReuse") {
		t.Error("should use ReadBytesSourceReuse for args")
	}
	if !strings.Contains(src, "argsReader.Reset") {
		t.Error("should reuse argsReader")
	}
}

func TestGenerate_PrivateReducer(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Reducers: []Reducer{
			{
				Name:       "InternalOp",
				Visibility: "private",
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}
	if !strings.Contains(string(out), "ReducerVisibilityPrivate") {
		t.Error("should set private visibility")
	}
}

func TestGenerate_ReducerNoParams(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Reducers: []Reducer{
			{Name: "Ping"},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "handlePing") {
		t.Error("should generate handlePing")
	}
	// No-param reducer should not read args
	if strings.Contains(src, "ReadBytesSourceReuse") {
		t.Error("no-param reducer should not read bytes source")
	}
}

func TestGenerate_Lifecycle(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Lifecycle: Lifecycle{
			OnInit:       "Init",
			OnConnect:    "OnConnect",
			OnDisconnect: "OnDisconnect",
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "handleInit") {
		t.Error("should generate handleInit")
	}
	if !strings.Contains(src, "handleOnConnect") {
		t.Error("should generate handleOnConnect")
	}
	if !strings.Contains(src, "handleOnDisconnect") {
		t.Error("should generate handleOnDisconnect")
	}
	if !strings.Contains(src, "LifecycleInit") {
		t.Error("should register LifecycleInit")
	}
	if !strings.Contains(src, "LifecycleOnConnect") {
		t.Error("should register LifecycleOnConnect")
	}
	if !strings.Contains(src, "LifecycleOnDisconnect") {
		t.Error("should register LifecycleOnDisconnect")
	}
}

func TestGenerate_Procedure(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Procedures: []Procedure{
			{
				Name: "GetData",
				Params: []Column{
					{Name: "key", Type: "String"},
				},
				ReturnType: "Bytes",
				Visibility: "public",
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, `Name: "GetData"`) {
		t.Error("should register procedure GetData")
	}
	if !strings.Contains(src, "handleGetDataProcedure") {
		t.Error("should generate handleGetDataProcedure")
	}
	if !strings.Contains(src, "ProcedureContext") {
		t.Error("should use ProcedureContext")
	}
}

func TestGenerate_ProcedureNoParams(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Procedures: []Procedure{
			{Name: "Health"},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "handleHealthProcedure") {
		t.Error("should generate handleHealthProcedure")
	}
}

func TestGenerate_View(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Views: []View{
			{
				Name:     "UserList",
				IsPublic: true,
				Params: []Column{
					{Name: "limit", Type: "U32"},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, `Name: "UserList"`) {
		t.Error("should register view UserList")
	}
	if !strings.Contains(src, "handleUserListView") {
		t.Error("should generate handleUserListView")
	}
	if !strings.Contains(src, "IsPublic:    true") {
		t.Error("should set IsPublic true")
	}
}

func TestGenerate_AnonymousView(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Views: []View{
			{
				Name:        "PublicFeed",
				IsAnonymous: true,
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "handlePublicFeedViewAnon") {
		t.Error("should generate anonymous view handler")
	}
	if !strings.Contains(src, "RegisterViewAnonHandler") {
		t.Error("should register anonymous view handler")
	}
}

func TestGenerate_CustomTypes(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name:    "Item",
				Columns: []Column{{Name: "id", Type: "U32"}},
			},
		},
		Types: []TypeExport{
			{
				Name: "Color",
				Sum: []SumVariant{
					{Name: "red", Type: ""},
					{Name: "green", Type: ""},
					{Name: "blue", Type: "U8"},
				},
			},
			{
				Name: "Point",
				Product: []Column{
					{Name: "x", Type: "F32"},
					{Name: "y", Type: "F32"},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, `Name:    "Color"`) {
		t.Error("should register Color type")
	}
	if !strings.Contains(src, `Name:    "Point"`) {
		t.Error("should register Point type")
	}
	if !strings.Contains(src, "RegisterTypespaceType") {
		t.Error("should call RegisterTypespaceType")
	}
}

func TestGenerate_AllColumnTypes(t *testing.T) {
	// Ensure all supported types can be generated without errors
	allTypes := []string{
		"String", "Bool", "I8", "U8", "I16", "U16",
		"I32", "U32", "I64", "U64", "F32", "F64",
		"Bytes", "Identity", "Timestamp",
		"Option<U32>", "Option<String>", "Option<Identity>", "Option<Timestamp>",
	}

	var columns []Column
	for i, typ := range allTypes {
		columns = append(columns, Column{
			Name: strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(typ, "<", "_"), ">", "")),
			Type: typ,
		})
		_ = i
	}

	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name:    "AllTypes",
				Columns: columns,
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate with all column types failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "type AllTypes struct") {
		t.Error("should generate AllTypes struct")
	}
	if !strings.Contains(src, "encodeAllTypes") {
		t.Error("should generate encodeAllTypes")
	}
	if !strings.Contains(src, "decodeAllTypes") {
		t.Error("should generate decodeAllTypes")
	}
}

func TestGenerate_SpecialWriteColumns(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "Msg",
				Columns: []Column{
					{Name: "sender", Type: "Identity"},
					{Name: "sent_at", Type: "Timestamp"},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	if !strings.Contains(src, "row.Sender.WriteBsatn(w)") {
		t.Error("Identity column should use WriteBsatn")
	}
	if !strings.Contains(src, "row.SentAt.WriteBsatn(w)") {
		t.Error("Timestamp column should use WriteBsatn")
	}
}

func TestGenerate_IdentityIndex(t *testing.T) {
	schema := Schema{
		Package: "testpkg",
		Tables: []Table{
			{
				Name: "Player",
				Columns: []Column{
					{Name: "identity", Type: "Identity"},
					{Name: "name", Type: "String"},
				},
				UniqueIndexes: []UniqueIndex{
					{Name: "idx_identity", Columns: []string{"identity"}},
				},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_name", Columns: []string{"name"}},
				},
			},
		},
	}
	out, err := generate(schema, "test.yaml")
	if err != nil {
		t.Fatalf("generate failed: %v", err)
	}

	src := string(out)
	// Unique index on Identity type should use WriteBsatn
	if !strings.Contains(src, "v.WriteBsatn(w)") {
		t.Error("Identity index key should use WriteBsatn")
	}
	if !strings.Contains(src, "types.Identity") {
		t.Error("Identity index should use types.Identity type")
	}
}

func TestGenerate_ComprehensiveSchema(t *testing.T) {
	// Full schema exercising all features together
	schema := Schema{
		Package: "mygame",
		Tables: []Table{
			{
				Name: "Player",
				Columns: []Column{
					{Name: "id", Type: "U64"},
					{Name: "identity", Type: "Identity"},
					{Name: "name", Type: "String"},
					{Name: "score", Type: "I32"},
					{Name: "last_seen", Type: "Option<Timestamp>"},
				},
				PrimaryKey: []string{"id"},
				UniqueIndexes: []UniqueIndex{
					{Name: "idx_identity", Columns: []string{"identity"}},
				},
				BTreeIndexes: []BTreeIndex{
					{Name: "idx_score", Columns: []string{"score"}},
					{Name: "idx_name_score", Columns: []string{"name", "score"}},
				},
				Access: "public",
			},
		},
		Reducers: []Reducer{
			{
				Name: "CreatePlayer",
				Params: []Column{
					{Name: "name", Type: "String"},
				},
			},
			{
				Name:       "Reset",
				Visibility: "private",
			},
		},
		Procedures: []Procedure{
			{
				Name: "FetchProfile",
				Params: []Column{
					{Name: "player_id", Type: "U64"},
				},
			},
		},
		Views: []View{
			{
				Name:     "Leaderboard",
				IsPublic: true,
			},
			{
				Name:        "AnonStats",
				IsAnonymous: true,
			},
		},
		Lifecycle: Lifecycle{
			OnInit:       "GameInit",
			OnConnect:    "PlayerConnect",
			OnDisconnect: "PlayerDisconnect",
		},
		Types: []TypeExport{
			{
				Name: "Direction",
				Sum: []SumVariant{
					{Name: "north", Type: ""},
					{Name: "south", Type: ""},
					{Name: "east", Type: ""},
					{Name: "west", Type: ""},
				},
			},
		},
	}
	out, err := generate(schema, "game.yaml")
	if err != nil {
		t.Fatalf("comprehensive generate failed: %v", err)
	}

	src := string(out)

	// Verify it's valid Go (gofmt succeeded)
	if !strings.Contains(src, "package mygame") {
		t.Error("should have correct package name")
	}

	// Check that all major features are present
	checks := []struct {
		desc, substr string
	}{
		{"Player struct", "type Player struct"},
		{"encode function", "func encodePlayer("},
		{"decode function", "func decodePlayer("},
		{"table handle", "playerTable"},
		{"unique index", "FindPlayerByIdentity"},
		{"unique delete", "DeletePlayerByIdentity"},
		{"unique update", "UpdatePlayerByIdentity"},
		{"btree filter", "FilterPlayerByScore"},
		{"btree range", "FilterPlayerByScoreRange"},
		{"prefix filter", "FilterPlayerByNameAndScoreRange"},
		{"reducer handler", "handleCreatePlayer"},
		{"private reducer", "handleReset"},
		{"procedure handler", "handleFetchProfileProcedure"},
		{"view handler", "handleLeaderboardView"},
		{"anon view handler", "handleAnonStatsViewAnon"},
		{"lifecycle init", "handleGameInit"},
		{"lifecycle connect", "handlePlayerConnect"},
		{"lifecycle disconnect", "handlePlayerDisconnect"},
		{"custom type", `Name:    "Direction"`},
		{"source file", "game.yaml"},
	}
	for _, c := range checks {
		if !strings.Contains(src, c.substr) {
			t.Errorf("comprehensive schema: missing %s (expected %q)", c.desc, c.substr)
		}
	}
}
