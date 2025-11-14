(** Tests for primitive type encoding/decoding *)

open Avro_simple

(** Helper to check binary encoding matches expected bytes *)
let check_encoding name codec value expected_bytes =
  let encoded = Codec.encode_to_bytes codec value in
  let hex_encoded =
    Bytes.to_seq encoded
    |> Seq.map (fun c -> Printf.sprintf "%02x" (Char.code c))
    |> List.of_seq
    |> String.concat " " in
  let hex_expected = String.concat " " expected_bytes in
  Alcotest.(check string) name hex_expected hex_encoded

(** Helper for roundtrip tests *)
let check_roundtrip name testable codec value =
  let encoded = Codec.encode_to_bytes codec value in
  let decoded = Codec.decode_from_bytes codec encoded in
  Alcotest.check testable name value decoded

(* ========== NULL TESTS ========== *)

let test_null_roundtrip () =
  check_roundtrip "null roundtrip" Alcotest.unit Codec.null ()

let test_null_encoding () =
  (* Null has no encoding - zero bytes *)
  let encoded = Codec.encode_to_bytes Codec.null () in
  Alcotest.(check int) "null encoding is empty" 0 (Bytes.length encoded)

(* ========== BOOLEAN TESTS ========== *)

let test_boolean_true () =
  check_roundtrip "boolean true" Alcotest.bool Codec.boolean true

let test_boolean_false () =
  check_roundtrip "boolean false" Alcotest.bool Codec.boolean false

let test_boolean_encoding () =
  (* Spec: false = 0x00, true = 0x01 *)
  check_encoding "false encoding" Codec.boolean false ["00"];
  check_encoding "true encoding" Codec.boolean true ["01"]

(* ========== INT TESTS ========== *)

let test_int_zero () =
  check_roundtrip "int zero" Alcotest.int Codec.int 0

let test_int_positive () =
  check_roundtrip "int 42" Alcotest.int Codec.int 42;
  check_roundtrip "int 100" Alcotest.int Codec.int 100;
  check_roundtrip "int 12345" Alcotest.int Codec.int 12345

let test_int_negative () =
  check_roundtrip "int -1" Alcotest.int Codec.int (-1);
  check_roundtrip "int -42" Alcotest.int Codec.int (-42);
  check_roundtrip "int -12345" Alcotest.int Codec.int (-12345)

let test_int_boundaries () =
  (* Test large positive and negative values *)
  check_roundtrip "int max_int/2" Alcotest.int Codec.int (max_int / 2);
  check_roundtrip "int min_int/2" Alcotest.int Codec.int (min_int / 2)

let test_int_zigzag_encoding () =
  (* Avro spec examples for zigzag encoding:
     0 -> 0x00
     -1 -> 0x01
     1 -> 0x02
     -2 -> 0x03
     2 -> 0x04
     -64 -> 0x7f
     64 -> 0x80 0x01
  *)
  check_encoding "int 0" Codec.int 0 ["00"];
  check_encoding "int -1" Codec.int (-1) ["01"];
  check_encoding "int 1" Codec.int 1 ["02"];
  check_encoding "int -2" Codec.int (-2) ["03"];
  check_encoding "int 2" Codec.int 2 ["04"];
  check_encoding "int -64" Codec.int (-64) ["7f"];
  check_encoding "int 64" Codec.int 64 ["80"; "01"]

(* ========== LONG TESTS ========== *)

let test_long_zero () =
  check_roundtrip "long zero" Alcotest.int64 Codec.long 0L

let test_long_positive () =
  check_roundtrip "long 42" Alcotest.int64 Codec.long 42L;
  check_roundtrip "long 1234567890" Alcotest.int64 Codec.long 1234567890L

let test_long_negative () =
  check_roundtrip "long -1" Alcotest.int64 Codec.long (-1L);
  check_roundtrip "long -1234567890" Alcotest.int64 Codec.long (-1234567890L)

let test_long_boundaries () =
  check_roundtrip "long max_int64/2" Alcotest.int64 Codec.long
    (Int64.div Int64.max_int 2L);
  check_roundtrip "long min_int64/2" Alcotest.int64 Codec.long
    (Int64.div Int64.min_int 2L)

let test_long_zigzag_encoding () =
  (* Same zigzag pattern as int but with long encoding *)
  check_encoding "long 0" Codec.long 0L ["00"];
  check_encoding "long -1" Codec.long (-1L) ["01"];
  check_encoding "long 1" Codec.long 1L ["02"];
  check_encoding "long -2" Codec.long (-2L) ["03"];
  check_encoding "long 2" Codec.long 2L ["04"]

(* ========== FLOAT TESTS ========== *)

let float_testable =
  let epsilon = 0.00001 in
  let equal a b =
    (Float.is_nan a && Float.is_nan b) ||
    (Float.is_infinite a && Float.is_infinite b && (a > 0.0) = (b > 0.0)) ||
    (Float.abs (a -. b) < epsilon)
  in
  Alcotest.testable (fun fmt f -> Format.fprintf fmt "%g" f) equal

let test_float_zero () =
  check_roundtrip "float 0.0" float_testable Codec.float 0.0

let test_float_positive () =
  check_roundtrip "float 3.14" float_testable Codec.float 3.14

let test_float_negative () =
  check_roundtrip "float -3.14" float_testable Codec.float (-3.14)

let test_float_special () =
  (* Note: NaN doesn't equal itself, so we test separately *)
  check_roundtrip "float infinity" float_testable Codec.float infinity;
  check_roundtrip "float neg_infinity" float_testable Codec.float neg_infinity

let test_float_nan () =
  let encoded = Codec.encode_to_bytes Codec.float nan in
  let decoded = Codec.decode_from_bytes Codec.float encoded in
  Alcotest.(check bool) "float nan roundtrip" true (Float.is_nan decoded)

(* ========== DOUBLE TESTS ========== *)

let test_double_zero () =
  check_roundtrip "double 0.0" float_testable Codec.double 0.0

let test_double_positive () =
  check_roundtrip "double 3.141592653589793" float_testable Codec.double 3.141592653589793

let test_double_negative () =
  check_roundtrip "double -3.141592653589793" float_testable Codec.double (-3.141592653589793)

let test_double_special () =
  check_roundtrip "double infinity" float_testable Codec.double infinity;
  check_roundtrip "double neg_infinity" float_testable Codec.double neg_infinity

let test_double_nan () =
  let encoded = Codec.encode_to_bytes Codec.double nan in
  let decoded = Codec.decode_from_bytes Codec.double encoded in
  Alcotest.(check bool) "double nan roundtrip" true (Float.is_nan decoded)

(* ========== BYTES TESTS ========== *)

let bytes_testable =
  Alcotest.testable
    (fun fmt b -> Format.fprintf fmt "%S" (Bytes.to_string b))
    Bytes.equal

let test_bytes_empty () =
  check_roundtrip "bytes empty" bytes_testable Codec.bytes (Bytes.empty)

let test_bytes_short () =
  check_roundtrip "bytes short" bytes_testable Codec.bytes (Bytes.of_string "hello")

let test_bytes_long () =
  let long_bytes = Bytes.create 1000 in
  for i = 0 to 999 do
    Bytes.set long_bytes i (Char.chr (i mod 256))
  done;
  check_roundtrip "bytes 1000" bytes_testable Codec.bytes long_bytes

let test_bytes_binary () =
  (* Test with non-ASCII bytes *)
  let binary = Bytes.of_string "\x00\x01\x02\xff\xfe\xfd" in
  check_roundtrip "bytes binary" bytes_testable Codec.bytes binary

(* ========== STRING TESTS ========== *)

let test_string_empty () =
  check_roundtrip "string empty" Alcotest.string Codec.string ""

let test_string_ascii () =
  check_roundtrip "string ascii" Alcotest.string Codec.string "hello world"

let test_string_unicode () =
  check_roundtrip "string unicode" Alcotest.string Codec.string "Hello 世界 🌍"

let test_string_long () =
  let long_string = String.make 1000 'a' in
  check_roundtrip "string 1000 chars" Alcotest.string Codec.string long_string

let test_string_special_chars () =
  check_roundtrip "string with newlines" Alcotest.string Codec.string "line1\nline2\r\nline3";
  check_roundtrip "string with tabs" Alcotest.string Codec.string "col1\tcol2\tcol3"

let test_string_encoding () =
  (* String "foo" should be: length(3) + "foo"
     length 3 zigzag encoded = 6 = 0x06 *)
  check_encoding "string 'foo'" Codec.string "foo" ["06"; "66"; "6f"; "6f"]

(* ========== FIXED TESTS ========== *)

let test_fixed_4 () =
  let codec = Codec.fixed 4 in
  let value = Bytes.of_string "\x01\x02\x03\x04" in
  check_roundtrip "fixed 4 bytes" bytes_testable codec value

let test_fixed_16 () =
  (* Common use case: UUID is 16 bytes *)
  let codec = Codec.fixed ~name:"UUID" 16 in
  let value = Bytes.create 16 in
  for i = 0 to 15 do
    Bytes.set value i (Char.chr i)
  done;
  check_roundtrip "fixed 16 bytes (UUID)" bytes_testable codec value

let test_fixed_encoding () =
  (* Fixed doesn't have length prefix - just the raw bytes *)
  let codec = Codec.fixed 3 in
  check_encoding "fixed 3 bytes" codec (Bytes.of_string "abc") ["61"; "62"; "63"]

let test_fixed_size_mismatch () =
  let codec = Codec.fixed 4 in
  let wrong_size = Bytes.of_string "abc" in  (* Only 3 bytes *)
  try
    let _ = Codec.encode_to_bytes codec wrong_size in
    Alcotest.fail "Should have raised exception for size mismatch"
  with Failure msg ->
    Alcotest.(check bool) "error message contains 'size mismatch'"
      true (String.contains msg 's')

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Primitives" [
    "null", [
      test_case "roundtrip" `Quick test_null_roundtrip;
      test_case "encoding" `Quick test_null_encoding;
    ];

    "boolean", [
      test_case "true" `Quick test_boolean_true;
      test_case "false" `Quick test_boolean_false;
      test_case "encoding" `Quick test_boolean_encoding;
    ];

    "int", [
      test_case "zero" `Quick test_int_zero;
      test_case "positive" `Quick test_int_positive;
      test_case "negative" `Quick test_int_negative;
      test_case "boundaries" `Quick test_int_boundaries;
      test_case "zigzag encoding" `Quick test_int_zigzag_encoding;
    ];

    "long", [
      test_case "zero" `Quick test_long_zero;
      test_case "positive" `Quick test_long_positive;
      test_case "negative" `Quick test_long_negative;
      test_case "boundaries" `Quick test_long_boundaries;
      test_case "zigzag encoding" `Quick test_long_zigzag_encoding;
    ];

    "float", [
      test_case "zero" `Quick test_float_zero;
      test_case "positive" `Quick test_float_positive;
      test_case "negative" `Quick test_float_negative;
      test_case "special values" `Quick test_float_special;
      test_case "nan" `Quick test_float_nan;
    ];

    "double", [
      test_case "zero" `Quick test_double_zero;
      test_case "positive" `Quick test_double_positive;
      test_case "negative" `Quick test_double_negative;
      test_case "special values" `Quick test_double_special;
      test_case "nan" `Quick test_double_nan;
    ];

    "bytes", [
      test_case "empty" `Quick test_bytes_empty;
      test_case "short" `Quick test_bytes_short;
      test_case "long" `Quick test_bytes_long;
      test_case "binary" `Quick test_bytes_binary;
    ];

    "string", [
      test_case "empty" `Quick test_string_empty;
      test_case "ascii" `Quick test_string_ascii;
      test_case "unicode" `Quick test_string_unicode;
      test_case "long" `Quick test_string_long;
      test_case "special chars" `Quick test_string_special_chars;
      test_case "encoding" `Quick test_string_encoding;
    ];

    "fixed", [
      test_case "4 bytes" `Quick test_fixed_4;
      test_case "16 bytes (UUID)" `Quick test_fixed_16;
      test_case "encoding" `Quick test_fixed_encoding;
      test_case "size mismatch" `Quick test_fixed_size_mismatch;
    ];
  ]
