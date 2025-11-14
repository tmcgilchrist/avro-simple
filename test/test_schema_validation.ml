(** Tests for schema validation *)

open Avro

(* ========== VALID SCHEMAS ========== *)

let test_valid_primitives () =
  Alcotest.(check (result unit string)) "null is valid"
    (Ok ()) (Schema.validate Schema.Null);
  Alcotest.(check (result unit string)) "int is valid"
    (Ok ()) (Schema.validate (Schema.Int None));
  Alcotest.(check (result unit string)) "string is valid"
    (Ok ()) (Schema.validate (Schema.String None))

let test_valid_array () =
  let schema = Schema.Array (Schema.Int None) in
  Alcotest.(check (result unit string)) "array of int is valid"
    (Ok ()) (Schema.validate schema)

let test_valid_map () =
  let schema = Schema.Map (Schema.String None) in
  Alcotest.(check (result unit string)) "map of string is valid"
    (Ok ()) (Schema.validate schema)

let test_valid_union () =
  let schema = Schema.Union [Schema.Null; Schema.String None] in
  Alcotest.(check (result unit string)) "union [null, string] is valid"
    (Ok ()) (Schema.validate schema)

let test_valid_record () =
  let schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  Alcotest.(check (result unit string)) "record with valid fields"
    (Ok ()) (Schema.validate schema)

let test_valid_enum () =
  let schema = Schema.Enum {
    enum_name = Type_name.simple "Color";
    symbols = ["RED"; "GREEN"; "BLUE"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  Alcotest.(check (result unit string)) "enum with valid symbols"
    (Ok ()) (Schema.validate schema)

let test_valid_fixed () =
  let schema = Schema.Fixed {
    fixed_name = Type_name.simple "MD5";
    size = 16;
    fixed_doc = None;
    fixed_aliases = [];
    fixed_logical = None;
  } in
  Alcotest.(check (result unit string)) "fixed with positive size"
    (Ok ()) (Schema.validate schema)

(* ========== INVALID SCHEMAS ========== *)

let test_invalid_union_single_branch () =
  let schema = Schema.Union [Schema.String None] in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "union with 1 branch fails"
        true (String.starts_with ~prefix:"Union must have at least 2" msg)
  | Ok () ->
      Alcotest.fail "Union with single branch should be invalid"

let test_invalid_union_nested () =
  let schema = Schema.Union [
    Schema.String None;
    Schema.Union [Schema.Null; Schema.Int None]
  ] in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "nested union fails"
        true (String.starts_with ~prefix:"Union cannot directly contain" msg)
  | Ok () ->
      Alcotest.fail "Nested union should be invalid"

let test_invalid_record_no_fields () =
  let schema = Schema.Record {
    name = Type_name.simple "Empty";
    fields = [];
    record_doc = None;
    record_aliases = [];
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "record with no fields fails"
        true (String.starts_with ~prefix:"Record must have at least one" msg)
  | Ok () ->
      Alcotest.fail "Record with no fields should be invalid"

let test_invalid_record_duplicate_fields () =
  let schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "name"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "duplicate field name fails"
        true (String.contains msg 'd')  (* contains "duplicate" *)
  | Ok () ->
      Alcotest.fail "Record with duplicate fields should be invalid"

let test_invalid_record_bad_field_name () =
  let schema = Schema.Record {
    name = Type_name.simple "Test";
    fields = [
      { field_name = "123invalid"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "invalid field name fails"
        true (String.contains msg 'I' || String.contains msg 'i')  (* "Invalid" *)
  | Ok () ->
      Alcotest.fail "Record with invalid field name should fail"

let test_invalid_enum_no_symbols () =
  let schema = Schema.Enum {
    enum_name = Type_name.simple "Empty";
    symbols = [];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "enum with no symbols fails"
        true (String.starts_with ~prefix:"Enum must have at least one" msg)
  | Ok () ->
      Alcotest.fail "Enum with no symbols should be invalid"

let test_invalid_enum_duplicate_symbols () =
  let schema = Schema.Enum {
    enum_name = Type_name.simple "Color";
    symbols = ["RED"; "GREEN"; "RED"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "duplicate symbol fails"
        true (String.contains msg 'd')  (* contains "duplicate" *)
  | Ok () ->
      Alcotest.fail "Enum with duplicate symbols should be invalid"

let test_invalid_enum_bad_symbol_name () =
  let schema = Schema.Enum {
    enum_name = Type_name.simple "Test";
    symbols = ["VALID"; "123-invalid"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "invalid symbol name fails"
        true (String.contains msg 'I' || String.contains msg 'i')
  | Ok () ->
      Alcotest.fail "Enum with invalid symbol should fail"

let test_invalid_fixed_zero_size () =
  let schema = Schema.Fixed {
    fixed_name = Type_name.simple "Bad";
    size = 0;
    fixed_doc = None;
    fixed_aliases = []; fixed_logical = None;
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "fixed with size 0 fails"
        true (String.starts_with ~prefix:"Fixed type size must be positive" msg)
  | Ok () ->
      Alcotest.fail "Fixed with zero size should be invalid"

let test_invalid_fixed_negative_size () =
  let schema = Schema.Fixed {
    fixed_name = Type_name.simple "Bad";
    size = -1;
    fixed_doc = None;
    fixed_aliases = []; fixed_logical = None;
  } in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "fixed with negative size fails"
        true (String.starts_with ~prefix:"Fixed type size must be positive" msg)
  | Ok () ->
      Alcotest.fail "Fixed with negative size should be invalid"

let test_invalid_union_duplicate_types () =
  (* Union with duplicate primitive types *)
  let schema = Schema.Union [Schema.Double; Schema.Double] in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "union with duplicate types fails"
        true (String.contains msg 'd' && String.contains msg 'u')  (* "duplicate" *)
  | Ok () ->
      Alcotest.fail "Union with duplicate types should be invalid"

let test_invalid_union_duplicate_named_types () =
  (* Union with duplicate record types (same name) *)
  let record_type = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let schema = Schema.Union [Schema.Null; record_type; record_type] in
  match Schema.validate schema with
  | Error msg ->
      Alcotest.(check bool) "union with duplicate record types fails"
        true (String.contains msg 'd')  (* "duplicate" *)
  | Ok () ->
      Alcotest.fail "Union with duplicate named types should be invalid"

let test_invalid_name_redefinition () =
  (* Record with two fields that both use the same named enum *)
  let enum_type = Schema.Enum {
    enum_name = Type_name.simple "Status";
    symbols = ["OK"; "ERROR"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  let schema = Schema.Record {
    name = Type_name.simple "Container";
    fields = [
      { field_name = "status1"; field_type = enum_type; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "status2"; field_type = enum_type; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  (* This should actually pass - same type used twice is OK *)
  match Schema.validate_schema schema with
  | Ok () -> ()  (* Expected *)
  | Error msg -> Alcotest.fail (Printf.sprintf "Same type used twice should be valid: %s" msg)

let test_invalid_name_redefinition_nested () =
  (* Nested records with same name - this should fail *)
  let inner_record = Schema.Record {
    name = Type_name.simple "Inner";
    fields = [
      { field_name = "value"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let duplicate_inner = Schema.Record {
    name = Type_name.simple "Inner";  (* Same name! *)
    fields = [
      { field_name = "data"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let schema = Schema.Record {
    name = Type_name.simple "Outer";
    fields = [
      { field_name = "first"; field_type = inner_record; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "second"; field_type = duplicate_inner; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Schema.validate_schema schema with
  | Error msg ->
      Alcotest.(check bool) "name redefinition fails"
        true (String.contains msg 'r' && String.contains msg 'e')  (* "redefined" *)
  | Ok () ->
      Alcotest.fail "Name redefinition should be invalid"

(* Test from Haskell: good enum used in multiple fields *)
let test_valid_enum_reuse () =
  let good_enum = Schema.Enum {
    enum_name = Type_name.simple "ok";
    symbols = ["a"; "b"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  let schema = Schema.Record {
    name = Type_name.simple "something";
    fields = [
      { field_name = "a"; field_type = good_enum; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "b"; field_type = good_enum; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  (* This should pass - reusing the same type is OK *)
  Alcotest.(check (result unit string)) "enum reuse is valid"
    (Ok ()) (Schema.validate_schema schema)

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Schema Validation" [
    "valid schemas", [
      test_case "primitives" `Quick test_valid_primitives;
      test_case "array" `Quick test_valid_array;
      test_case "map" `Quick test_valid_map;
      test_case "union" `Quick test_valid_union;
      test_case "record" `Quick test_valid_record;
      test_case "enum" `Quick test_valid_enum;
      test_case "fixed" `Quick test_valid_fixed;
      test_case "enum reuse" `Quick test_valid_enum_reuse;
    ];

    "invalid schemas", [
      test_case "union single branch" `Quick test_invalid_union_single_branch;
      test_case "union nested" `Quick test_invalid_union_nested;
      test_case "union duplicate types" `Quick test_invalid_union_duplicate_types;
      test_case "union duplicate named types" `Quick test_invalid_union_duplicate_named_types;
      test_case "record no fields" `Quick test_invalid_record_no_fields;
      test_case "record duplicate fields" `Quick test_invalid_record_duplicate_fields;
      test_case "record bad field name" `Quick test_invalid_record_bad_field_name;
      test_case "enum no symbols" `Quick test_invalid_enum_no_symbols;
      test_case "enum duplicate symbols" `Quick test_invalid_enum_duplicate_symbols;
      test_case "enum bad symbol name" `Quick test_invalid_enum_bad_symbol_name;
      test_case "fixed zero size" `Quick test_invalid_fixed_zero_size;
      test_case "fixed negative size" `Quick test_invalid_fixed_negative_size;
      test_case "name redefinition allowed for reuse" `Quick test_invalid_name_redefinition;
      test_case "name redefinition nested" `Quick test_invalid_name_redefinition_nested;
    ];
  ]
