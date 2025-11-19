(** Property-based tests using QCheck *)

open Avro_simple
open QCheck

(** Helper: Create a QCheck test that encode/decode roundtrips correctly *)
let test_roundtrip ~name ~count codec arb =
  Test.make ~name ~count arb (fun value ->
    let encoded = Codec.encode_to_bytes codec value in
    let decoded = Codec.decode_from_bytes codec encoded in
    value = decoded
  )

(* ========== PRIMITIVE TYPES ========== *)

let test_boolean_roundtrip =
  test_roundtrip ~name:"boolean roundtrip" ~count:1000 Codec.boolean bool

let test_int_roundtrip =
  test_roundtrip ~name:"int roundtrip" ~count:1000 Codec.int int

let test_long_roundtrip =
  test_roundtrip ~name:"long roundtrip" ~count:1000 Codec.long int64

let test_float_roundtrip =
  (* Use small floats to avoid precision issues *)
  let arb = make (Gen.float_range (-1000.0) 1000.0) in
  Test.make ~name:"float roundtrip" ~count:1000 arb (fun value ->
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
  Test.make ~name:"double roundtrip" ~count:1000 arb (fun value ->
    let encoded = Codec.encode_to_bytes Codec.double value in
    let decoded = Codec.decode_from_bytes Codec.double encoded in
    let epsilon = 0.00000001 in
    Float.abs (value -. decoded) < epsilon ||
    (Float.is_nan value && Float.is_nan decoded) ||
    (Float.is_infinite value && Float.is_infinite decoded && (value > 0.0) = (decoded > 0.0))
  )

let test_bytes_roundtrip =
  let arb = make (Gen.map Bytes.of_string Gen.string) in
  test_roundtrip ~name:"bytes roundtrip" ~count:1000 Codec.bytes arb

let test_string_roundtrip =
  test_roundtrip ~name:"string roundtrip" ~count:1000 Codec.string string

(* ========== COMPLEX TYPES ========== *)

let test_array_int_roundtrip =
  test_roundtrip ~name:"array int roundtrip" ~count:1000 (Codec.array Codec.int) (array int)

let test_array_string_roundtrip =
  test_roundtrip ~name:"array string roundtrip" ~count:1000 (Codec.array Codec.string) (array string)

let test_map_int_roundtrip =
  test_roundtrip ~name:"map int roundtrip" ~count:1000 (Codec.map Codec.int) (list (pair string int))

let test_option_string_roundtrip =
  test_roundtrip ~name:"option string roundtrip" ~count:1000 (Codec.option Codec.string) (option string)

let test_option_int_roundtrip =
  test_roundtrip ~name:"option int roundtrip" ~count:1000 (Codec.option Codec.int) (option int)

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
  Test.make ~name:"fixed roundtrip" ~count:100 arb (fun value ->
    let codec = Codec.fixed 16 in
    let encoded = Codec.encode_to_bytes codec value in
    let decoded = Codec.decode_from_bytes codec encoded in
    Bytes.equal value decoded
  )

(* ========== SPECIAL CASES ========== *)

let test_empty_string_roundtrip =
  Test.make ~name:"empty string roundtrip" ~count:10 unit (fun () ->
    let value = "" in
    let encoded = Codec.encode_to_bytes Codec.string value in
    let decoded = Codec.decode_from_bytes Codec.string encoded in
    value = decoded
  )

let test_large_int_roundtrip =
  let arb = make (Gen.int_range (min_int / 2) (max_int / 2)) in
  test_roundtrip ~name:"large int roundtrip" ~count:1000 Codec.int arb

let test_nested_array_roundtrip =
  test_roundtrip ~name:"nested array roundtrip" ~count:1000 (Codec.array (Codec.array Codec.int)) (array (array int))

(* ========== INVARIANTS ========== *)

(** Test that encoding size is reasonable *)
let test_string_encoding_size =
  Test.make ~name:"string encoding size" ~count:100 string (fun s ->
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
  Test.make ~name:"int compact encoding" ~count:100 arb (fun i ->
    let encoded = Codec.encode_to_bytes Codec.int i in
    (* Small ints should encode to 1 byte *)
    Bytes.length encoded <= 2
  )

(* ========== RUN TESTS ========== *)

let () =
  let open Alcotest in

  let qcheck ?(speed=`Quick) test = QCheck_alcotest.to_alcotest ~speed_level:speed test in

  run "QCheck Properties" [
    "primitives", [
      qcheck test_boolean_roundtrip;
      qcheck test_int_roundtrip;
      qcheck test_long_roundtrip;
      qcheck test_float_roundtrip;
      qcheck test_double_roundtrip;
      qcheck test_bytes_roundtrip;
      qcheck test_string_roundtrip;
    ];

    "complex types", [
      qcheck test_array_int_roundtrip;
      qcheck test_array_string_roundtrip;
      qcheck test_map_int_roundtrip;
      qcheck test_option_string_roundtrip;
      qcheck test_option_int_roundtrip;
    ];

    "special cases", [
      qcheck test_fixed_roundtrip;
      qcheck test_empty_string_roundtrip;
      qcheck test_large_int_roundtrip;
      qcheck ~speed:`Slow test_nested_array_roundtrip;
    ];

    "invariants", [
      qcheck test_string_encoding_size;
      qcheck test_int_compact_encoding;
    ];
  ]
