(** Encoding benchmarks *)

open Benchmark

let bench_int_encoding () =
  let open Avro in
  let value = 42 in
  ignore (Codec.encode_to_bytes Codec.int value)

let bench_string_encoding () =
  let open Avro in
  let value = "hello world this is a test string" in
  ignore (Codec.encode_to_bytes Codec.string value)

let bench_array_encoding () =
  let open Avro in
  let value = Array.init 1000 (fun i -> i) in
  let codec = Codec.array Codec.int in
  ignore (Codec.encode_to_bytes codec value)

let () =
  let results = throughputN 3 [
    "int encoding", bench_int_encoding, ();
    "string encoding", bench_string_encoding, ();
    "array encoding", bench_array_encoding, ();
  ] in
  tabulate results
