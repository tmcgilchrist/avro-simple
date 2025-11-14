(** Property-based tests using QCheck *)

open Avro_simple
open QCheck

(** Helper: Test that encode/decode roundtrips correctly *)
let test_roundtrip codec arb =
  Test.make ~count:1000 arb (fun value ->
    let encoded = Codec.encode_to_bytes codec value in
    let decoded = Codec.decode_from_bytes codec encoded in
    value = decoded
  )

(* ========== PRIMITIVE TYPES ========== *)

let test_boolean_roundtrip =
  test_roundtrip Codec.boolean bool

let test_int_roundtrip =
  test_roundtrip Codec.int int

let test_long_roundtrip =
  test_roundtrip Codec.long int64

let test_float_roundtrip =
  (* Use small floats to avoid precision issues *)
  let arb = make (Gen.float_range (-1000.0) 1000.0) in
  Test.make ~count:1000 arb (fun value ->
    let encoded = Codec.encode_to_bytes Codec.float value in
    let decoded = Codec.decode_from_bytes Codec.float encoded in
    (* Use epsilon comparison for floats *)
    let epsilon = 0.0001 in
    Float.abs (value -. decoded) < epsilon ||
    (Float.is_nan value && Float.is_nan decoded) ||
    (Float.is_infinite value && Float.is_infinite decoded && (value > 0.0) = (decoded > 0.0))
  )

let test_double_roundtrip =
  let arb = make (Gen.float_range (-1000.0) 1000.0) in
  Test.make ~count:1000 arb (fun value ->
    let encoded = Codec.encode_to_bytes Codec.double value in
    let decoded = Codec.decode_from_bytes Codec.double encoded in
    let epsilon = 0.00000001 in
    Float.abs (value -. decoded) < epsilon ||
    (Float.is_nan value && Float.is_nan decoded) ||
    (Float.is_infinite value && Float.is_infinite decoded && (value > 0.0) = (decoded > 0.0))
  )

let test_bytes_roundtrip =
  let arb = make (Gen.map Bytes.of_string Gen.string) in
  test_roundtrip Codec.bytes arb

let test_string_roundtrip =
  test_roundtrip Codec.string string

(* ========== COMPLEX TYPES ========== *)

let test_array_int_roundtrip =
  test_roundtrip (Codec.array Codec.int) (array int)

let test_array_string_roundtrip =
  test_roundtrip (Codec.array Codec.string) (array string)

let test_map_int_roundtrip =
  test_roundtrip (Codec.map Codec.int) (list (pair string int))

let test_option_string_roundtrip =
  test_roundtrip (Codec.option Codec.string) (option string)

let test_option_int_roundtrip =
  test_roundtrip (Codec.option Codec.int) (option int)

(* ========== FIXED TYPE ========== *)

let test_fixed_roundtrip =
  let gen_fixed size =
    Gen.map (fun s ->
      let bytes = Bytes.create size in
      String.iteri (fun i c ->
        if i < size then Bytes.set bytes i c
      ) s;
      (* Pad with zeros if string is shorter *)
      bytes
    ) Gen.string
  in
  let arb = make (gen_fixed 16) in
  Test.make ~count:100 arb (fun value ->
    let codec = Codec.fixed 16 in
    let encoded = Codec.encode_to_bytes codec value in
    let decoded = Codec.decode_from_bytes codec encoded in
    Bytes.equal value decoded
  )

(* ========== SPECIAL CASES ========== *)

let test_empty_string_roundtrip =
  Test.make ~count:10 unit (fun () ->
    let value = "" in
    let encoded = Codec.encode_to_bytes Codec.string value in
    let decoded = Codec.decode_from_bytes Codec.string encoded in
    value = decoded
  )

let test_large_int_roundtrip =
  let arb = make (Gen.int_range (min_int / 2) (max_int / 2)) in
  test_roundtrip Codec.int arb

let test_nested_array_roundtrip =
  test_roundtrip (Codec.array (Codec.array Codec.int)) (array (array int))

(* ========== INVARIANTS ========== *)

(** Test that encoding size is reasonable *)
let test_string_encoding_size =
  Test.make ~count:100 string (fun s ->
    let encoded = Codec.encode_to_bytes Codec.string s in
    (* Encoded size should be roughly: varint_length + string_length *)
    (* For strings < 128 chars, varint is 1 byte *)
    if String.length s < 128 then
      Bytes.length encoded <= String.length s + 10  (* Some overhead for length encoding *)
    else
      true  (* Larger strings can have variable overhead *)
  )

(** Test that int encoding is compact for small values *)
let test_int_compact_encoding =
  let arb = make (Gen.int_range 0 127) in
  Test.make ~count:100 arb (fun i ->
    let encoded = Codec.encode_to_bytes Codec.int i in
    (* Small ints should encode to 1 byte *)
    Bytes.length encoded <= 2
  )

(* ========== RUN TESTS ========== *)

let () =
  let open QCheck_base_runner in
  QCheck_base_runner.set_seed 12345;
  let suite = [
    test_boolean_roundtrip;
    test_int_roundtrip;
    test_long_roundtrip;
    test_float_roundtrip;
    test_double_roundtrip;
    test_bytes_roundtrip;
    test_string_roundtrip;
    test_array_int_roundtrip;
    test_array_string_roundtrip;
    test_map_int_roundtrip;
    test_option_string_roundtrip;
    test_option_int_roundtrip;
    test_fixed_roundtrip;
    test_empty_string_roundtrip;
    test_large_int_roundtrip;
    test_nested_array_roundtrip;
    test_string_encoding_size;
    test_int_compact_encoding;
  ] in
  let results = run_tests suite in
  exit (if results = 0 then 0 else 1)
