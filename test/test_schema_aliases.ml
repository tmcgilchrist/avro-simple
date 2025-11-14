(** Tests for schema aliases in schema resolution *)

open Avro_simple

(* ========== ENUM ALIASES ========== *)

let test_enum_renamed_type () =
  (* Writer uses old name "Color", reader uses new name "Colour" with alias *)
  let writer_schema = Schema.Enum {
    enum_name = Type_name.simple "Color";
    symbols = ["RED"; "GREEN"; "BLUE"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in

  let reader_schema = Schema.Enum {
    enum_name = Type_name.simple "Colour";
    symbols = ["RED"; "GREEN"; "BLUE"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = ["Color"];  (* Old name as alias *)
  } in

  (* Should successfully resolve *)
  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error _ -> Alcotest.fail "Enum with alias should resolve successfully"

let test_enum_renamed_with_namespace () =
  (* Test with namespaced names *)
  let writer_schema = Schema.Enum {
    enum_name = Type_name.parse "com.example.Status";
    symbols = ["OK"; "ERROR"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in

  let reader_schema = Schema.Enum {
    enum_name = Type_name.parse "org.newco.State";
    symbols = ["OK"; "ERROR"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = ["com.example.Status"];  (* Fully qualified old name *)
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error _ -> Alcotest.fail "Enum with namespaced alias should resolve"

let test_enum_no_alias_fails () =
  (* Different names without alias should fail *)
  let writer_schema = Schema.Enum {
    enum_name = Type_name.simple "Color";
    symbols = ["RED"; "GREEN"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in

  let reader_schema = Schema.Enum {
    enum_name = Type_name.simple "Colour";
    symbols = ["RED"; "GREEN"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];  (* No alias! *)
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Error _ -> ()  (* Expected *)
  | Ok _ -> Alcotest.fail "Enum without matching alias should fail"

(* ========== FIXED ALIASES ========== *)

let test_fixed_renamed_type () =
  let writer_schema = Schema.Fixed {
    fixed_name = Type_name.simple "MD5";
    size = 16;
    fixed_doc = None;
    fixed_aliases = [];
    fixed_logical = None;
  } in

  let reader_schema = Schema.Fixed {
    fixed_name = Type_name.simple "Hash";
    size = 16;
    fixed_doc = None;
    fixed_aliases = ["MD5"];  (* Old name as alias *)
    fixed_logical = None;
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error _ -> Alcotest.fail "Fixed with alias should resolve successfully"

let test_fixed_wrong_size_with_alias_fails () =
  (* Even with alias, size mismatch should fail *)
  let writer_schema = Schema.Fixed {
    fixed_name = Type_name.simple "MD5";
    size = 16;
    fixed_doc = None;
    fixed_aliases = [];
    fixed_logical = None;
  } in

  let reader_schema = Schema.Fixed {
    fixed_name = Type_name.simple "Hash";
    size = 32;  (* Different size! *)
    fixed_doc = None;
    fixed_aliases = ["MD5"];
    fixed_logical = None;
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Error (Resolution.Fixed_size_mismatch _) -> ()  (* Expected *)
  | Error e -> Alcotest.fail (Printf.sprintf "Expected size mismatch error, got: %s"
                               (Resolution.error_to_string e))
  | Ok _ -> Alcotest.fail "Fixed with size mismatch should fail even with alias"

(* ========== RECORD ALIASES ========== *)

let test_record_renamed_type () =
  let writer_schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  let reader_schema = Schema.Record {
    name = Type_name.simple "Employee";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = ["Person"];  (* Old name as alias *)
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error e -> Alcotest.fail (Printf.sprintf "Record with alias should resolve: %s"
                               (Resolution.error_to_string e))

let test_record_multiple_aliases () =
  (* Reader has multiple aliases for the same type *)
  let writer_schema = Schema.Record {
    name = Type_name.simple "OldName";
    fields = [
      { field_name = "value"; field_type = Schema.Int None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  let reader_schema = Schema.Record {
    name = Type_name.simple "NewName";
    fields = [
      { field_name = "value"; field_type = Schema.Int None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = ["OldName"; "MiddleName"; "AnotherOldName"];
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error _ -> Alcotest.fail "Record with multiple aliases should resolve"

(* ========== FIELD ALIASES ========== *)

let test_field_renamed () =
  (* Writer has field "firstName", reader has "name" with alias *)
  let writer_schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "firstName"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  let reader_schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = ["firstName"];  (* Old field name *) };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error e -> Alcotest.fail (Printf.sprintf "Field with alias should resolve: %s"
                               (Resolution.error_to_string e))

let test_field_multiple_aliases () =
  let writer_schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "first_name"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  let reader_schema = Schema.Record {
    name = Type_name.simple "Person";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = ["firstName"; "first_name"; "fname"];  };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error _ -> Alcotest.fail "Field with multiple aliases should resolve"

let test_field_and_type_aliases_together () =
  (* Both the record type AND fields have been renamed *)
  let writer_schema = Schema.Record {
    name = Type_name.simple "OldPerson";
    fields = [
      { field_name = "firstName"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None;
        field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  let reader_schema = Schema.Record {
    name = Type_name.simple "NewPerson";
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None;
        field_default = None; field_aliases = ["firstName"];  };
      { field_name = "yearsOld"; field_type = Schema.Int None; field_doc = None;
        field_default = None; field_aliases = ["age"];  };
    ];
    record_doc = None;
    record_aliases = ["OldPerson"];  (* Type alias *)
  } in

  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok _ -> ()
  | Error e -> Alcotest.fail (Printf.sprintf
      "Type and field aliases together should resolve: %s"
      (Resolution.error_to_string e))

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Schema Aliases" [
    "enum aliases", [
      test_case "renamed enum type" `Quick test_enum_renamed_type;
      test_case "enum with namespace alias" `Quick test_enum_renamed_with_namespace;
      test_case "enum without alias fails" `Quick test_enum_no_alias_fails;
    ];

    "fixed aliases", [
      test_case "renamed fixed type" `Quick test_fixed_renamed_type;
      test_case "wrong size fails even with alias" `Quick test_fixed_wrong_size_with_alias_fails;
    ];

    "record aliases", [
      test_case "renamed record type" `Quick test_record_renamed_type;
      test_case "multiple record aliases" `Quick test_record_multiple_aliases;
    ];

    "field aliases", [
      test_case "renamed field" `Quick test_field_renamed;
      test_case "multiple field aliases" `Quick test_field_multiple_aliases;
      test_case "type and field aliases together" `Quick test_field_and_type_aliases_together;
    ];
  ]
