(** Tests for complex types (arrays, maps, unions) *)

let test_array () =
  let open Avro_simple in
  let value = [| 1; 2; 3; 4; 5 |] in
  let codec = Codec.array Codec.int in
  let encoded = Codec.encode_to_bytes codec value in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check (array int)) "array roundtrip" value decoded

let test_empty_array () =
  let open Avro_simple in
  let codec = Codec.array Codec.string in
  let arr = [||] in
  let encoded = Codec.encode_to_bytes codec arr in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check (array string)) "empty array roundtrip" arr decoded

(* Test map codec *)
let test_map_single () =
  let open Avro_simple in
  let codec = Codec.map Codec.string in
  let map = [("key", "value")] in
  let encoded = Codec.encode_to_bytes codec map in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check int) "map has 1 entry" 1 (List.length decoded);
  let value = List.assoc "key" decoded in
  Alcotest.(check string) "value matches" "value" value

let test_map_multiple () =
  let open Avro_simple in
  let codec = Codec.map Codec.int in
  let map = [("one", 1); ("two", 2); ("three", 3)] in
  let encoded = Codec.encode_to_bytes codec map in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check int) "map has 3 entries" 3 (List.length decoded);
  Alcotest.(check int) "one = 1" 1 (List.assoc "one" decoded);
  Alcotest.(check int) "two = 2" 2 (List.assoc "two" decoded);
  Alcotest.(check int) "three = 3" 3 (List.assoc "three" decoded)

let test_map_empty () =
  let open Avro_simple in
  let codec = Codec.map Codec.string in
  let map = [] in
  let encoded = Codec.encode_to_bytes codec map in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check int) "empty map" 0 (List.length decoded)

let test_option_some () =
  let open Avro_simple in
  let value = Some "hello" in
  let codec = Codec.option Codec.string in
  let encoded = Codec.encode_to_bytes codec value in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check (option string)) "option some roundtrip" value decoded

let test_option_none () =
  let open Avro_simple in
  let value = None in
  let codec = Codec.option Codec.string in
  let encoded = Codec.encode_to_bytes codec value in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.(check (option string)) "option none roundtrip" value decoded

let () =
  let open Alcotest in
  run "Complex Types" [
    "arrays", [
      test_case "array" `Quick test_array;
      test_case "empty array" `Quick test_empty_array;
    ];
    "maps", [
      test_case "single entry" `Quick test_map_single;
      test_case "multiple entries" `Quick test_map_multiple;
      test_case "empty map" `Quick test_map_empty;
    ];
    "unions", [
      test_case "option some" `Quick test_option_some;
      test_case "option none" `Quick test_option_none;
    ];
  ]
