(** Tests for schema evolution *)

open Avro_simple

(* ========== PRIMITIVE TYPE MATCHING ========== *)

let test_null_to_null () =
  match Resolution.resolve_schemas Schema.Null Schema.Null with
  | Ok Resolution.Null -> ()
  | _ -> Alcotest.fail "Null should resolve to Null"

let test_boolean_to_boolean () =
  match Resolution.resolve_schemas Schema.Boolean Schema.Boolean with
  | Ok Resolution.Boolean -> ()
  | _ -> Alcotest.fail "Boolean should resolve to Boolean"

let test_int_to_int () =
  match Resolution.resolve_schemas (Schema.Int None) (Schema.Int None) with
  | Ok Resolution.Int -> ()
  | _ -> Alcotest.fail "Int should resolve to Int"

let test_string_to_string () =
  match Resolution.resolve_schemas (Schema.String None) (Schema.String None) with
  | Ok Resolution.String -> ()
  | _ -> Alcotest.fail "String should resolve to String"

(* ========== TYPE PROMOTIONS ========== *)

let test_int_to_long () =
  match Resolution.resolve_schemas (Schema.Long None) (Schema.Int None) with
  | Ok Resolution.Int_as_long -> ()
  | _ -> Alcotest.fail "Int should promote to Long"

let test_int_to_float () =
  match Resolution.resolve_schemas Schema.Float (Schema.Int None) with
  | Ok Resolution.Int_as_float -> ()
  | _ -> Alcotest.fail "Int should promote to Float"

let test_int_to_double () =
  match Resolution.resolve_schemas Schema.Double (Schema.Int None) with
  | Ok Resolution.Int_as_double -> ()
  | _ -> Alcotest.fail "Int should promote to Double"

let test_long_to_float () =
  match Resolution.resolve_schemas Schema.Float (Schema.Long None) with
  | Ok Resolution.Long_as_float -> ()
  | _ -> Alcotest.fail "Long should promote to Float"

let test_long_to_double () =
  match Resolution.resolve_schemas Schema.Double (Schema.Long None) with
  | Ok Resolution.Long_as_double -> ()
  | _ -> Alcotest.fail "Long should promote to Double"

let test_float_to_double () =
  match Resolution.resolve_schemas Schema.Double Schema.Float with
  | Ok Resolution.Float_as_double -> ()
  | _ -> Alcotest.fail "Float should promote to Double"

(* ========== STRING/BYTES COMPATIBILITY ========== *)

let test_string_to_bytes () =
  match Resolution.resolve_schemas (Schema.Bytes None) (Schema.String None) with
  | Ok Resolution.Bytes -> ()
  | _ -> Alcotest.fail "String should be readable as Bytes"

let test_bytes_to_string () =
  match Resolution.resolve_schemas (Schema.String None) (Schema.Bytes None) with
  | Ok Resolution.String -> ()
  | _ -> Alcotest.fail "Bytes should be readable as String"

(* ========== INCOMPATIBLE TYPES ========== *)

let test_int_to_string_fails () =
  match Resolution.resolve_schemas (Schema.String None) (Schema.Int None) with
  | Error (Resolution.Type_mismatch _) -> ()
  | _ -> Alcotest.fail "Int to String should fail"

let test_long_to_int_fails () =
  match Resolution.resolve_schemas (Schema.Int None) (Schema.Long None) with
  | Error (Resolution.Type_mismatch _) -> ()
  | _ -> Alcotest.fail "Long to Int should fail (no demotion)"

let test_double_to_float_fails () =
  match Resolution.resolve_schemas Schema.Float Schema.Double with
  | Error (Resolution.Type_mismatch _) -> ()
  | _ -> Alcotest.fail "Double to Float should fail (no demotion)"

(* ========== ARRAYS ========== *)

let test_array_int_to_int () =
  let reader = Schema.Array (Schema.Int None) in
  let writer = Schema.Array (Schema.Int None) in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Array Resolution.Int) -> ()
  | _ -> Alcotest.fail "Array<int> should resolve to Array<int>"

let test_array_int_to_long () =
  let reader = Schema.Array (Schema.Long None) in
  let writer = Schema.Array (Schema.Int None) in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Array Resolution.Int_as_long) -> ()
  | _ -> Alcotest.fail "Array<int> should promote to Array<long>"

let test_array_incompatible_fails () =
  let reader = Schema.Array (Schema.String None) in
  let writer = Schema.Array (Schema.Int None) in
  match Resolution.resolve_schemas reader writer with
  | Error (Resolution.Type_mismatch _) -> ()
  | _ -> Alcotest.fail "Array<int> to Array<string> should fail"

(* ========== MAPS ========== *)

let test_map_string_to_string () =
  let reader = Schema.Map (Schema.String None) in
  let writer = Schema.Map (Schema.String None) in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Map Resolution.String) -> ()
  | _ -> Alcotest.fail "Map<string> should resolve to Map<string>"

let test_map_int_to_long () =
  let reader = Schema.Map (Schema.Long None) in
  let writer = Schema.Map (Schema.Int None) in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Map Resolution.Int_as_long) -> ()
  | _ -> Alcotest.fail "Map<int> should promote to Map<long>"

(* ========== FIXED ========== *)

let test_fixed_same_size () =
  let name = Type_name.simple "MD5" in
  let reader = Schema.Fixed { fixed_name = name; size = 16; fixed_doc = None; fixed_aliases = []; fixed_logical = None } in
  let writer = Schema.Fixed { fixed_name = name; size = 16; fixed_doc = None; fixed_aliases = []; fixed_logical = None } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Fixed (_, 16)) -> ()
  | _ -> Alcotest.fail "Fixed(16) should resolve to Fixed(16)"

let test_fixed_different_size_fails () =
  let name = Type_name.simple "Fixed" in
  let reader = Schema.Fixed { fixed_name = name; size = 16; fixed_doc = None; fixed_aliases = []; fixed_logical = None } in
  let writer = Schema.Fixed { fixed_name = name; size = 32; fixed_doc = None; fixed_aliases = []; fixed_logical = None } in
  match Resolution.resolve_schemas reader writer with
  | Error (Resolution.Fixed_size_mismatch _) -> ()
  | _ -> Alcotest.fail "Fixed with different sizes should fail"

(* ========== RECORDS ========== *)

let test_record_same_fields () =
  let name = Type_name.simple "Person" in
  let reader = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let writer = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Record { fields; defaults; _ }) ->
      Alcotest.(check int) "2 fields" 2 (List.length fields);
      Alcotest.(check int) "no defaults" 0 (List.length defaults)
  | _ -> Alcotest.fail "Record should resolve"

let test_record_add_optional_field () =
  let name = Type_name.simple "Person" in
  let reader = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = Some (Schema.Int_default 0); field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let writer = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Record { fields; defaults; _ }) ->
      Alcotest.(check int) "1 writer field" 1 (List.length fields);
      Alcotest.(check int) "1 default" 1 (List.length defaults)
  | _ -> Alcotest.fail "Adding field with default should work"

let test_record_remove_field () =
  let name = Type_name.simple "Person" in
  let reader = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let writer = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Record { fields; _ }) ->
      (* Should have 2 fields: name (with position) and age (position = None to skip) *)
      Alcotest.(check int) "2 fields in resolution" 2 (List.length fields);
      let age_field = List.find (fun f -> f.Resolution.field_name = "age") fields in
      (match age_field.Resolution.field_position with
      | None -> ()  (* Correct - should skip this field *)
      | Some _ -> Alcotest.fail "Removed field should have position = None")
  | _ -> Alcotest.fail "Removing field should work"

let test_record_missing_required_field_fails () =
  let name = Type_name.simple "Person" in
  let reader = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };  (* Required! *)
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let writer = Schema.Record {
    name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Resolution.resolve_schemas reader writer with
  | Error (Resolution.Missing_field _) -> ()
  | _ -> Alcotest.fail "Missing required field should fail"

let test_record_field_type_promotion () =
  let name = Type_name.simple "Person" in
  let reader = Schema.Record {
    name;
    fields = [
      { field_name = "age"; field_type = Schema.Long None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  let writer = Schema.Record {
    name;
    fields = [
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Record { fields; _ }) ->
      let age_field = List.hd fields in
      (match age_field.Resolution.field_schema with
      | Resolution.Int_as_long -> ()
      | _ -> Alcotest.fail "Field should promote int to long")
  | _ -> Alcotest.fail "Field promotion should work"

(* ========== ENUMS ========== *)

let test_enum_same_symbols () =
  let name = Type_name.simple "Color" in
  let reader = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"; "BLUE"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  let writer = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"; "BLUE"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Enum { symbol_map; _ }) ->
      Alcotest.(check int) "3 symbols" 3 (Array.length symbol_map);
      Alcotest.(check int) "RED maps to 0" 0 symbol_map.(0);
      Alcotest.(check int) "GREEN maps to 1" 1 symbol_map.(1);
      Alcotest.(check int) "BLUE maps to 2" 2 symbol_map.(2)
  | _ -> Alcotest.fail "Enum should resolve"

let test_enum_reordered_symbols () =
  let name = Type_name.simple "Color" in
  let reader = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"; "BLUE"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  let writer = Schema.Enum { enum_name = name; symbols = ["GREEN"; "BLUE"; "RED"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Enum { symbol_map; _ }) ->
      (* Writer GREEN(0) -> Reader GREEN(1) *)
      Alcotest.(check int) "GREEN maps correctly" 1 symbol_map.(0);
      (* Writer BLUE(1) -> Reader BLUE(2) *)
      Alcotest.(check int) "BLUE maps correctly" 2 symbol_map.(1);
      (* Writer RED(2) -> Reader RED(0) *)
      Alcotest.(check int) "RED maps correctly" 0 symbol_map.(2)
  | _ -> Alcotest.fail "Reordered enum should resolve"

let test_enum_missing_symbol_fails () =
  let name = Type_name.simple "Color" in
  let reader = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  let writer = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"; "BLUE"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  match Resolution.resolve_schemas reader writer with
  | Error (Resolution.Missing_symbol "BLUE") -> ()
  | _ -> Alcotest.fail "Missing symbol should fail"

let test_enum_default_symbol () =
  let name = Type_name.simple "Color" in
  (* Reader has default symbol "RED" for missing symbols *)
  let reader = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"]; enum_doc = None; enum_default = Some "RED"; enum_aliases = [] } in
  let writer = Schema.Enum { enum_name = name; symbols = ["RED"; "GREEN"; "BLUE"]; enum_doc = None; enum_default = None; enum_aliases = [] } in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Enum { symbol_map; _ }) ->
      (* Writer BLUE(2) -> Reader RED(0) via default *)
      Alcotest.(check int) "BLUE maps to default RED" 0 symbol_map.(2)
  | _ -> Alcotest.fail "Enum with default symbol should resolve"

(* ========== UNIONS ========== *)

let test_union_same_branches () =
  let reader = Schema.Union [Schema.Null; Schema.String None] in
  let writer = Schema.Union [Schema.Null; Schema.String None] in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Union branches) ->
      Alcotest.(check int) "2 branches" 2 (Array.length branches);
      let (idx0, _) = branches.(0) in
      let (idx1, _) = branches.(1) in
      Alcotest.(check int) "Null at index 0" 0 idx0;
      Alcotest.(check int) "String at index 1" 1 idx1
  | _ -> Alcotest.fail "Union should resolve"

let test_union_reordered_branches () =
  let reader = Schema.Union [Schema.Null; Schema.String None] in
  let writer = Schema.Union [Schema.String None; Schema.Null] in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.Union branches) ->
      let (idx0, _) = branches.(0) in  (* Writer String should map to reader index 1 *)
      let (idx1, _) = branches.(1) in  (* Writer Null should map to reader index 0 *)
      Alcotest.(check int) "Writer String -> Reader index 1" 1 idx0;
      Alcotest.(check int) "Writer Null -> Reader index 0" 0 idx1
  | _ -> Alcotest.fail "Reordered union should resolve"

let test_non_union_to_union () =
  let reader = Schema.Union [Schema.Null; Schema.String None] in
  let writer = (Schema.String None) in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.As_union (idx, Resolution.String)) ->
      Alcotest.(check int) "String wraps to index 1" 1 idx
  | _ -> Alcotest.fail "Non-union should wrap in union"

let test_non_union_to_union_with_promotion () =
  let reader = Schema.Union [Schema.Null; Schema.Long None] in
  let writer = (Schema.Int None) in
  match Resolution.resolve_schemas reader writer with
  | Ok (Resolution.As_union (idx, Resolution.Int_as_long)) ->
      Alcotest.(check int) "Int promotes and wraps to Long at index 1" 1 idx
  | _ -> Alcotest.fail "Non-union should promote and wrap"

let test_union_missing_branch_fails () =
  let reader = Schema.Union [Schema.Null; Schema.String None] in
  let writer = Schema.Union [Schema.Null; Schema.String None; Schema.Int None] in
  match Resolution.resolve_schemas reader writer with
  | Error (Resolution.Missing_union_branch _) -> ()
  | _ -> Alcotest.fail "Missing union branch should fail"

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Schema Evolution" [
    "primitives", [
      test_case "null to null" `Quick test_null_to_null;
      test_case "boolean to boolean" `Quick test_boolean_to_boolean;
      test_case "int to int" `Quick test_int_to_int;
      test_case "string to string" `Quick test_string_to_string;
    ];

    "promotions", [
      test_case "int to long" `Quick test_int_to_long;
      test_case "int to float" `Quick test_int_to_float;
      test_case "int to double" `Quick test_int_to_double;
      test_case "long to float" `Quick test_long_to_float;
      test_case "long to double" `Quick test_long_to_double;
      test_case "float to double" `Quick test_float_to_double;
    ];

    "string/bytes", [
      test_case "string to bytes" `Quick test_string_to_bytes;
      test_case "bytes to string" `Quick test_bytes_to_string;
    ];

    "incompatible", [
      test_case "int to string fails" `Quick test_int_to_string_fails;
      test_case "long to int fails" `Quick test_long_to_int_fails;
      test_case "double to float fails" `Quick test_double_to_float_fails;
    ];

    "arrays", [
      test_case "array int to int" `Quick test_array_int_to_int;
      test_case "array int promotes to long" `Quick test_array_int_to_long;
      test_case "array incompatible fails" `Quick test_array_incompatible_fails;
    ];

    "maps", [
      test_case "map string to string" `Quick test_map_string_to_string;
      test_case "map int promotes to long" `Quick test_map_int_to_long;
    ];

    "fixed", [
      test_case "same size" `Quick test_fixed_same_size;
      test_case "different size fails" `Quick test_fixed_different_size_fails;
    ];

    "records", [
      test_case "same fields" `Quick test_record_same_fields;
      test_case "add optional field" `Quick test_record_add_optional_field;
      test_case "remove field" `Quick test_record_remove_field;
      test_case "missing required field fails" `Quick test_record_missing_required_field_fails;
      test_case "field type promotion" `Quick test_record_field_type_promotion;
    ];

    "enums", [
      test_case "same symbols" `Quick test_enum_same_symbols;
      test_case "reordered symbols" `Quick test_enum_reordered_symbols;
      test_case "missing symbol fails" `Quick test_enum_missing_symbol_fails;
      test_case "default symbol" `Quick test_enum_default_symbol;
    ];

    "unions", [
      test_case "same branches" `Quick test_union_same_branches;
      test_case "reordered branches" `Quick test_union_reordered_branches;
      test_case "non-union to union" `Quick test_non_union_to_union;
      test_case "non-union to union with promotion" `Quick test_non_union_to_union_with_promotion;
      test_case "missing branch fails" `Quick test_union_missing_branch_fails;
    ];
  ]
