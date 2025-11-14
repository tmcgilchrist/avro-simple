(** Tests for schema evolution with actual data encoding/decoding *)

open Avro_simple

(** Type promotion tests *)

let test_int_to_long () =
  let value = 42 in
  let bytes = Codec.encode_to_bytes Codec.int value in
  match Decoder.decode_with_schemas (Schema.Long None) (Schema.Int None) bytes with
  | Ok (Value.Long 42L) -> ()
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Long 42L, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed"

let test_int_to_double () =
  let value = 42 in
  let bytes = Codec.encode_to_bytes Codec.int value in
  match Decoder.decode_with_schemas Schema.Double (Schema.Int None) bytes with
  | Ok (Value.Double 42.0) -> ()
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Double 42.0, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed"

let test_float_to_double () =
  let value = 3.14 in
  let bytes = Codec.encode_to_bytes Codec.float value in
  match Decoder.decode_with_schemas Schema.Double Schema.Float bytes with
  | Ok (Value.Double d) ->
      (* Float precision means we need approximate comparison *)
      if abs_float (d -. 3.14) < 0.01 then ()
      else Alcotest.fail (Printf.sprintf "Expected ~3.14, got %f" d)
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Double, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed"

(** Array promotion test *)

let test_array_int_to_long () =
  let writer_codec = Codec.array Codec.int in
  let value = [| 1; 2; 3 |] in
  let bytes = Codec.encode_to_bytes writer_codec value in
  let reader_schema = Schema.Array (Schema.Long None) in
  match Decoder.decode_with_schemas reader_schema writer_codec.Codec.schema bytes with
  | Ok (Value.Array arr) ->
      if Array.length arr = 3 then
        (match arr.(0), arr.(1), arr.(2) with
        | Value.Long 1L, Value.Long 2L, Value.Long 3L -> ()
        | _ -> Alcotest.fail "Array elements should be promoted to long")
      else
        Alcotest.fail "Array length mismatch"
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Array, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed"

(** Record evolution tests *)

let test_record_remove_field () =
  (* Writer has: { name: string, age: int }
     Reader has: { name: string } *)

  let writer_name = Type_name.simple "Person" in
  let writer_codec =
    Codec.record writer_name (fun name age -> (name, age))
    |> Codec.field "name" Codec.string fst
    |> Codec.field "age" Codec.int snd
    |> Codec.finish
  in

  let value = ("Alice", 30) in
  let bytes = Codec.encode_to_bytes writer_codec value in

  let reader_schema = Schema.Record {
    name = writer_name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  match Decoder.decode_with_schemas reader_schema writer_codec.Codec.schema bytes with
  | Ok (Value.Record fields) ->
      (match List.assoc_opt "name" fields with
      | Some (Value.String "Alice") -> ()
      | Some v -> Alcotest.fail (Printf.sprintf "Expected String Alice, got %s" (Value.to_string v))
      | None -> Alcotest.fail "Missing name field")
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Record, got %s" (Value.to_string v))
  | Error e ->
      let msg = match e with
      | Resolution.Type_mismatch _ -> "Type mismatch"
      | Resolution.Missing_field (_, f) -> "Missing field: " ^ f
      | Resolution.Field_mismatch (_, f) -> "Field mismatch: " ^ f
      | _ -> "Other error"
      in
      Alcotest.fail ("Should succeed, got: " ^ msg)

let test_record_add_optional_field () =
  (* Writer has: { name: string }
     Reader has: { name: string, age: int = 0 } *)

  let type_name = Type_name.simple "Person" in
  let writer_codec =
    Codec.record type_name (fun name -> name)
    |> Codec.field "name" Codec.string (fun x -> x)
    |> Codec.finish
  in

  let value = "Alice" in
  let bytes = Codec.encode_to_bytes writer_codec value in

  let reader_schema = Schema.Record {
    name = type_name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Int None; field_doc = None; field_default = Some (Schema.Int_default 0); field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  match Decoder.decode_with_schemas reader_schema writer_codec.Codec.schema bytes with
  | Ok (Value.Record fields) ->
      (match List.assoc_opt "name" fields, List.assoc_opt "age" fields with
      | Some (Value.String "Alice"), Some (Value.Int 0) -> ()
      | _ -> Alcotest.fail "Expected name=Alice and age=0")
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Record, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed with default value"

(** Union promotion test *)

let test_non_union_to_union () =
  (* Writer has: int
     Reader has: union [null, long] *)

  let value = 42 in
  let bytes = Codec.encode_to_bytes Codec.int value in

  let reader_schema = Schema.Union [Schema.Null; Schema.Long None] in

  match Decoder.decode_with_schemas reader_schema (Schema.Int None) bytes with
  | Ok (Value.Union (branch, Value.Long 42L)) ->
      (* Should be wrapped in union, branch 1 (long), with promotion *)
      if branch = 1 then ()
      else Alcotest.fail (Printf.sprintf "Expected branch 1, got %d" branch)
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Union(1, Long 42), got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed"

(** Map promotion test *)

let test_map_int_to_double () =
  let writer_codec = Codec.map Codec.int in
  let value = [("a", 1); ("b", 2)] in
  let bytes = Codec.encode_to_bytes writer_codec value in
  let reader_schema = Schema.Map Schema.Double in
  match Decoder.decode_with_schemas reader_schema writer_codec.Codec.schema bytes with
  | Ok (Value.Map pairs) ->
      if List.length pairs = 2 then
        (match List.assoc_opt "a" pairs, List.assoc_opt "b" pairs with
        | Some (Value.Double 1.0), Some (Value.Double 2.0) -> ()
        | _ -> Alcotest.fail "Map values should be promoted to double")
      else
        Alcotest.fail "Map length mismatch"
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Map, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed"

(** Record with nested promotion *)

let test_record_nested_promotion () =
  (* Writer has: { name: string, age: int }
     Reader has: { name: string, age: long } *)

  let type_name = Type_name.simple "Person" in
  let writer_codec =
    Codec.record type_name (fun name age -> (name, age))
    |> Codec.field "name" Codec.string fst
    |> Codec.field "age" Codec.int snd
    |> Codec.finish
  in

  let value = ("Bob", 25) in
  let bytes = Codec.encode_to_bytes writer_codec value in

  let reader_schema = Schema.Record {
    name = type_name;
    fields = [
      { field_name = "name"; field_type = Schema.String None; field_doc = None; field_default = None; field_aliases = [] };
      { field_name = "age"; field_type = Schema.Long None; field_doc = None; field_default = None; field_aliases = [] };
    ];
    record_doc = None;
    record_aliases = [];
  } in

  match Decoder.decode_with_schemas reader_schema writer_codec.Codec.schema bytes with
  | Ok (Value.Record fields) ->
      (match List.assoc_opt "name" fields, List.assoc_opt "age" fields with
      | Some (Value.String "Bob"), Some (Value.Long 25L) -> ()
      | Some (Value.String "Bob"), Some v ->
          Alcotest.fail (Printf.sprintf "Age should be promoted to long, got %s" (Value.to_string v))
      | _ -> Alcotest.fail "Unexpected field values")
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Record, got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed with type promotion"

(** Union with promotion *)

let test_union_with_promotion () =
  (* Writer has: union [null, int]
     Reader has: union [null, long] *)

  let writer_codec = Codec.option Codec.int in
  let value = Some 42 in
  let bytes = Codec.encode_to_bytes writer_codec value in

  let reader_schema = Schema.Union [Schema.Null; Schema.Long None] in

  match Decoder.decode_with_schemas reader_schema writer_codec.Codec.schema bytes with
  | Ok (Value.Union (1, Value.Long 42L)) -> ()
  | Ok v -> Alcotest.fail (Printf.sprintf "Expected Union(1, Long 42), got %s" (Value.to_string v))
  | Error _ -> Alcotest.fail "Should succeed with promotion"

(** Test suite *)

let () =
  let open Alcotest in
  run "Schema Evolution Roundtrip" [
    "type promotions", [
      test_case "int to long" `Quick test_int_to_long;
      test_case "int to double" `Quick test_int_to_double;
      test_case "float to double" `Quick test_float_to_double;
    ];
    "arrays", [
      test_case "array int to long" `Quick test_array_int_to_long;
    ];
    "maps", [
      test_case "map int to double" `Quick test_map_int_to_double;
    ];
    "records", [
      test_case "remove field" `Quick test_record_remove_field;
      test_case "add optional field" `Quick test_record_add_optional_field;
      test_case "nested promotion" `Quick test_record_nested_promotion;
    ];
    "unions", [
      test_case "non-union to union" `Quick test_non_union_to_union;
      test_case "union with promotion" `Quick test_union_with_promotion;
    ];
  ]
