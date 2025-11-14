(** Cross-language benchmark - OCaml implementation

    This benchmark is designed to be comparable with Java, Rust, and Python
    implementations. It performs common Avro operations and outputs timing
    information that can be collected by hyperfine.
*)
open Avro_simple

type person = {
  name: string;
  age: int;
  email: string option;
  phone_numbers: string array;
}

(* TODO Make this randomised using Qcheck to generate data *)
let create_person i =
  {
    name = Printf.sprintf "Person_%d" i;
    age = 20 + (i mod 60);
    email = if i mod 3 = 0 then Some (Printf.sprintf "person%d@example.com" i) else None;
    phone_numbers = Array.init (1 + i mod 3) (fun j -> Printf.sprintf "+1-555-%04d" (i * 10 + j));
  }

let person_codec =
  Codec.record (Type_name.simple "Person")
    (fun name age email phone_numbers -> ({ name; age; email; phone_numbers } : person))
  |> Codec.field "name" Codec.string (fun (p : person) -> p.name)
  |> Codec.field "age" Codec.int (fun (p : person) -> p.age)
  |> Codec.field_opt "email" Codec.string (fun (p : person) -> p.email)
  |> Codec.field "phone_numbers" (Codec.array Codec.string) (fun (p : person) -> p.phone_numbers)
  |> Codec.finish

let benchmark_encode count =
  let people = Array.init count create_person in
  let start = Unix.gettimeofday () in
  let encoded = Array.map (fun p -> Avro.Codec.encode_to_bytes person_codec p) people in
  let elapsed = Unix.gettimeofday () -. start in
  let total_bytes = Array.fold_left (fun acc b -> acc + Bytes.length b) 0 encoded in
  Printf.printf "Encoded %d records in %.6f seconds (%.2f MB/s, %d bytes)\n"
    count elapsed
    (float_of_int total_bytes /. elapsed /. 1_000_000.0)
    total_bytes

let benchmark_decode count =
  let people = Array.init count create_person in
  let encoded = Array.map (fun p -> Avro.Codec.encode_to_bytes person_codec p) people in
  let start = Unix.gettimeofday () in
  let _decoded = Array.map (fun b -> Avro.Codec.decode_from_bytes person_codec b) encoded in
  let elapsed = Unix.gettimeofday () -. start in
  let total_bytes = Array.fold_left (fun acc b -> acc + Bytes.length b) 0 encoded in
  Printf.printf "Decoded %d records in %.6f seconds (%.2f MB/s, %d bytes)\n"
    count elapsed
    (float_of_int total_bytes /. elapsed /. 1_000_000.0)
    total_bytes

let benchmark_container count compression =
  Avro.init_codecs ();
  let people = Array.init count create_person in
  let filename = Printf.sprintf "test_cross_language_bench_%s.avro" compression in

  (* Write *)
  let start_write = Unix.gettimeofday () in
  let writer = Avro.Container_writer.create
    ~path:filename
    ~codec:person_codec
    ~compression
    () in
  Array.iter (fun p -> Avro.Container_writer.write writer p) people;
  Avro.Container_writer.close writer;
  let elapsed_write = Unix.gettimeofday () -. start_write in

  (* Read *)
  let start_read = Unix.gettimeofday () in
  let reader = Avro.Container_reader.open_file ~path:filename ~codec:person_codec () in
  let count_read = ref 0 in
  Avro.Container_reader.iter (fun _p -> incr count_read) reader;
  Avro.Container_reader.close reader;
  let elapsed_read = Unix.gettimeofday () -. start_read in

  let file_size = (Unix.stat filename).Unix.st_size in
  Sys.remove filename;

  Printf.printf "Container[%s]: Wrote %d records in %.6f seconds, Read in %.6f seconds (%d bytes)\n"
    compression count elapsed_write elapsed_read file_size

let () =
  let operation = if Array.length Sys.argv > 1 then Sys.argv.(1) else "encode" in
  let count = if Array.length Sys.argv > 2 then int_of_string Sys.argv.(2) else 10000 in
  let compression = if Array.length Sys.argv > 3 then Sys.argv.(3) else "null" in

  match operation with
  | "encode" -> benchmark_encode count
  | "decode" -> benchmark_decode count
  | "container" -> benchmark_container count compression
  | _ ->
    Printf.eprintf "Usage: %s [encode|decode|container] [count] [compression]\n" Sys.argv.(0);
    exit 1
