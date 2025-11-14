(** Decoding benchmarks *)

open Benchmark

let bench_int_decoding () =
  let open Avro_simple in
  let value = 42 in
  let encoded = Codec.encode_to_bytes Codec.int value in
  fun () -> ignore (Codec.decode_from_bytes Codec.int encoded)

let bench_string_decoding () =
  let open Avro_simple in
  let value = "hello world this is a test string" in
  let encoded = Codec.encode_to_bytes Codec.string value in
  fun () -> ignore (Codec.decode_from_bytes Codec.string encoded)

let bench_array_decoding () =
  let open Avro_simple in
  let value = Array.init 1000 (fun i -> i) in
  let codec = Codec.array Codec.int in
  let encoded = Codec.encode_to_bytes codec value in
  fun () -> ignore (Codec.decode_from_bytes codec encoded)

let () =
  let results = throughputN 3 [
    "int decoding", bench_int_decoding (), ();
    "string decoding", bench_string_decoding (), ();
    "array decoding", bench_array_decoding (), ();
  ] in
  tabulate results
