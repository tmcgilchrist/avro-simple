(** Tests for JSON schema parsing *)

open Avro_simple

(** Test parsing primitive types *)
let test_primitive_null () =
  let json = "\"null\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is null" true (schema = Schema.Null)
  | Error msg -> Alcotest.fail msg

let test_primitive_boolean () =
  let json = "\"boolean\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is boolean" true (schema = Schema.Boolean)
  | Error msg -> Alcotest.fail msg

let test_primitive_int () =
  let json = "\"int\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is int" true (schema = Schema.Int None)
  | Error msg -> Alcotest.fail msg

let test_primitive_long () =
  let json = "\"long\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is long" true (schema = Schema.Long None)
  | Error msg -> Alcotest.fail msg

let test_primitive_float () =
  let json = "\"float\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is float" true (schema = Schema.Float)
  | Error msg -> Alcotest.fail msg

let test_primitive_double () =
  let json = "\"double\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is double" true (schema = Schema.Double)
  | Error msg -> Alcotest.fail msg

let test_primitive_bytes () =
  let json = "\"bytes\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is bytes" true (schema = Schema.Bytes None)
  | Error msg -> Alcotest.fail msg

let test_primitive_string () =
  let json = "\"string\"" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is string" true (schema = Schema.String None)
  | Error msg -> Alcotest.fail msg

(** Test parsing object form of primitives *)
let test_primitive_object_form () =
  let json = "{\"type\": \"int\"}" in
  match Schema_json.of_string json with
  | Ok schema -> Alcotest.(check bool) "is int" true (schema = Schema.Int None)
  | Error msg -> Alcotest.fail msg

(** Test parsing array schema *)
let test_array () =
  let json = "{\"type\": \"array\", \"items\": \"string\"}" in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Array (Schema.String None) -> ()
      | _ -> Alcotest.fail "Expected array of string"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing map schema *)
let test_map () =
  let json = "{\"type\": \"map\", \"values\": \"int\"}" in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Map (Schema.Int None) -> ()
      | _ -> Alcotest.fail "Expected map of int"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing union schema *)
let test_union () =
  let json = "[\"null\", \"string\", \"int\"]" in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Union [Schema.Null; Schema.String None; Schema.Int None] -> ()
      | _ -> Alcotest.fail "Expected union [null, string, int]"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing empty union *)
let test_empty_union () =
  let json = "[]" in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Union [] -> ()
      | _ -> Alcotest.fail "Expected empty union"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing simple record *)
let test_record_simple () =
  let json = {|
    {
      "type": "record",
      "name": "Person",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
      ]
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Record { name; fields; _ } ->
          Alcotest.(check string) "record name" "Person" name.Type_name.name;
          Alcotest.(check int) "field count" 2 (List.length fields);
          let f1 = List.nth fields 0 in
          let f2 = List.nth fields 1 in
          Alcotest.(check string) "field 1 name" "name" f1.field_name;
          Alcotest.(check bool) "field 1 type" true (f1.field_type = Schema.String None);
          Alcotest.(check string) "field 2 name" "age" f2.field_name;
          Alcotest.(check bool) "field 2 type" true (f2.field_type = Schema.Int None)
      | _ -> Alcotest.fail "Expected record"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing record with namespace *)
let test_record_namespace () =
  let json = {|
    {
      "type": "record",
      "namespace": "com.example",
      "name": "Person",
      "fields": []
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Record { name; _ } ->
          Alcotest.(check string) "full name" "com.example.Person" (Type_name.full_name name)
      | _ -> Alcotest.fail "Expected record"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing record with doc *)
let test_record_with_doc () =
  let json = {|
    {
      "type": "record",
      "name": "Person",
      "doc": "A person record",
      "fields": []
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Record { record_doc = Some doc; _ } ->
          Alcotest.(check string) "doc" "A person record" doc
      | _ -> Alcotest.fail "Expected record with doc"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing record with field defaults *)
let test_record_field_defaults () =
  let json = {|
    {
      "type": "record",
      "name": "Person",
      "fields": [
        {"name": "name", "type": "string", "default": "John"},
        {"name": "age", "type": "int", "default": 30}
      ]
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Record { fields; _ } ->
          let f1 = List.nth fields 0 in
          let f2 = List.nth fields 1 in
          begin match f1.field_default with
          | Some (Schema.String_default "John") -> ()
          | _ -> Alcotest.fail "Expected string default"
          end;
          begin match f2.field_default with
          | Some (Schema.Int_default 30) -> ()
          | _ -> Alcotest.fail "Expected int default"
          end
      | _ -> Alcotest.fail "Expected record"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing enum *)
let test_enum () =
  let json = {|
    {
      "type": "enum",
      "name": "Suit",
      "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Enum { enum_name; symbols; _ } ->
          Alcotest.(check string) "enum name" "Suit" enum_name.Type_name.name;
          Alcotest.(check int) "symbol count" 4 (List.length symbols);
          Alcotest.(check string) "first symbol" "SPADES" (List.nth symbols 0)
      | _ -> Alcotest.fail "Expected enum"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing enum with namespace *)
let test_enum_namespace () =
  let json = {|
    {
      "type": "enum",
      "namespace": "com.example",
      "name": "Suit",
      "symbols": ["SPADES", "HEARTS"]
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Enum { enum_name; _ } ->
          Alcotest.(check string) "full name" "com.example.Suit" (Type_name.full_name enum_name)
      | _ -> Alcotest.fail "Expected enum"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing enum with default *)
let test_enum_default () =
  let json = {|
    {
      "type": "enum",
      "name": "Suit",
      "symbols": ["SPADES", "HEARTS"],
      "default": "SPADES"
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Enum { enum_default = Some default; _ } ->
          Alcotest.(check string) "default symbol" "SPADES" default
      | _ -> Alcotest.fail "Expected enum with default"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing fixed *)
let test_fixed () =
  let json = {|
    {
      "type": "fixed",
      "name": "MD5",
      "size": 16
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Fixed { fixed_name; size; _ } ->
          Alcotest.(check string) "fixed name" "MD5" fixed_name.Type_name.name;
          Alcotest.(check int) "size" 16 size
      | _ -> Alcotest.fail "Expected fixed"
      end
  | Error msg -> Alcotest.fail msg

(** Test parsing fixed with namespace *)
let test_fixed_namespace () =
  let json = {|
    {
      "type": "fixed",
      "namespace": "com.example",
      "name": "MD5",
      "size": 16
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Fixed { fixed_name; _ } ->
          Alcotest.(check string) "full name" "com.example.MD5" (Type_name.full_name fixed_name)
      | _ -> Alcotest.fail "Expected fixed"
      end
  | Error msg -> Alcotest.fail msg

(** Test complex nested schema *)
let test_nested_complex () =
  let json = {|
    {
      "type": "record",
      "name": "Person",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "emails", "type": {"type": "array", "items": "string"}},
        {"name": "address", "type": ["null", {
          "type": "record",
          "name": "Address",
          "fields": [
            {"name": "street", "type": "string"},
            {"name": "city", "type": "string"}
          ]
        }]}
      ]
    }
  |} in
  match Schema_json.of_string json with
  | Ok schema ->
      begin match schema with
      | Schema.Record { name; fields; _ } ->
          Alcotest.(check string) "record name" "Person" name.Type_name.name;
          Alcotest.(check int) "field count" 4 (List.length fields);

          (* Check emails field is array of string *)
          let emails_field = List.nth fields 2 in
          begin match emails_field.field_type with
          | Schema.Array (Schema.String None) -> ()
          | _ -> Alcotest.fail "Expected array of string"
          end;

          (* Check address field is union with null and record *)
          let address_field = List.nth fields 3 in
          begin match address_field.field_type with
          | Schema.Union [Schema.Null; Schema.Record { name = addr_name; fields = addr_fields; _ }] ->
              Alcotest.(check string) "address record name" "Address" addr_name.Type_name.name;
              Alcotest.(check int) "address field count" 2 (List.length addr_fields)
          | _ -> Alcotest.fail "Expected union with null and record"
          end
      | _ -> Alcotest.fail "Expected record"
      end
  | Error msg -> Alcotest.fail msg

(** Test roundtrip: schema -> JSON -> schema *)
let test_roundtrip_simple () =
  let original = Schema.Record {
    name = Type_name.simple "Test";
    fields = [
      {
        field_name = "value";
        field_type = Schema.Int None;
        field_doc = None;
        field_default = None;
        field_aliases = [];
      }
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let json_str = Schema_json.to_string original in
  match Schema_json.of_string json_str with
  | Ok parsed ->
      begin match parsed with
      | Schema.Record { name; fields; _ } ->
          Alcotest.(check string) "name matches" "Test" name.Type_name.name;
          Alcotest.(check int) "field count" 1 (List.length fields);
          let field = List.hd fields in
          Alcotest.(check string) "field name" "value" field.field_name;
          Alcotest.(check bool) "field type" true (field.field_type = Schema.Int None)
      | _ -> Alcotest.fail "Expected record"
      end
  | Error msg -> Alcotest.fail ("Roundtrip failed: " ^ msg)

(** Test roundtrip with enum *)
let test_roundtrip_enum () =
  let original = Schema.Enum {
    enum_name = Type_name.simple "Color";
    symbols = ["RED"; "GREEN"; "BLUE"];
    enum_doc = None;
    enum_default = None;
    enum_aliases = [];
  } in
  let json_str = Schema_json.to_string original in
  match Schema_json.of_string json_str with
  | Ok parsed ->
      begin match parsed with
      | Schema.Enum { enum_name; symbols; _ } ->
          Alcotest.(check string) "name matches" "Color" enum_name.Type_name.name;
          Alcotest.(check int) "symbol count" 3 (List.length symbols)
      | _ -> Alcotest.fail "Expected enum"
      end
  | Error msg -> Alcotest.fail ("Roundtrip failed: " ^ msg)

(** Test canonical form matches expected format *)
let test_canonical_form () =
  let schema = Schema.Record {
    name = Type_name.simple "test";
    fields = [
      {
        field_name = "a";
        field_type = Schema.Long None;
        field_doc = Some "The field a";  (* Should be stripped *)
        field_default = Some (Schema.Int_default 42);  (* Should be stripped *)
        field_aliases = [];
      };
      {
        field_name = "b";
        field_type = Schema.String None;
        field_doc = None;
        field_default = None;
        field_aliases = [];
      }
    ];
    record_doc = Some "A test record";  (* Should be stripped *)
    record_aliases = [];
  } in
  let json_str = Schema_json.to_string schema in
  let expected = "{\"name\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}" in
  Alcotest.(check string) "canonical form" expected json_str

let () =
  let open Alcotest in
  run "Schema JSON" [
    "primitives", [
      test_case "null" `Quick test_primitive_null;
      test_case "boolean" `Quick test_primitive_boolean;
      test_case "int" `Quick test_primitive_int;
      test_case "long" `Quick test_primitive_long;
      test_case "float" `Quick test_primitive_float;
      test_case "double" `Quick test_primitive_double;
      test_case "bytes" `Quick test_primitive_bytes;
      test_case "string" `Quick test_primitive_string;
      test_case "object form" `Quick test_primitive_object_form;
    ];
    "complex types", [
      test_case "array" `Quick test_array;
      test_case "map" `Quick test_map;
      test_case "union" `Quick test_union;
      test_case "empty union" `Quick test_empty_union;
    ];
    "records", [
      test_case "simple record" `Quick test_record_simple;
      test_case "record with namespace" `Quick test_record_namespace;
      test_case "record with doc" `Quick test_record_with_doc;
      test_case "record with field defaults" `Quick test_record_field_defaults;
    ];
    "enums", [
      test_case "simple enum" `Quick test_enum;
      test_case "enum with namespace" `Quick test_enum_namespace;
      test_case "enum with default" `Quick test_enum_default;
    ];
    "fixed", [
      test_case "simple fixed" `Quick test_fixed;
      test_case "fixed with namespace" `Quick test_fixed_namespace;
    ];
    "nested", [
      test_case "complex nested" `Quick test_nested_complex;
    ];
    "roundtrip", [
      test_case "simple roundtrip" `Quick test_roundtrip_simple;
      test_case "enum roundtrip" `Quick test_roundtrip_enum;
    ];
    "canonical", [
      test_case "canonical form" `Quick test_canonical_form;
    ];
  ]
