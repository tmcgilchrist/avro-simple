(** Compression codec benchmarks *)

open Avro_simple

type person = {
  name: string;
  age: int;
  email: string option;
  phone_numbers: string array;
}

let person_codec =
  Codec.record (Type_name.simple "Person")
    (fun name age email phone_numbers -> ({ name; age; email; phone_numbers } : person))
  |> Codec.field "name" Codec.string (fun (p : person) -> p.name)
  |> Codec.field "age" Codec.int (fun (p : person) -> p.age)
  |> Codec.field_opt "email" Codec.string (fun (p : person) -> p.email)
  |> Codec.field "phone_numbers" (Codec.array Codec.string) (fun (p : person) -> p.phone_numbers)
  |> Codec.finish

(* Maybe this should be generated data or we have a generator that writes the file
   and then consume it. *)
let create_person i =
  {
    name = Printf.sprintf "Person_%d" i;
    age = 20 + (i mod 60);
    email = (if i mod 3 = 0 then Some (Printf.sprintf "person%d@example.com" i) else None);
    phone_numbers = Array.init (1 + i mod 3) (fun j -> Printf.sprintf "+1-555-%04d" (i * 10 + j));
  }

let benchmark_compression count compression null_size_opt =
  let temp_path = Printf.sprintf "test_compression_bench_%s.avro" compression in
  let people = Array.init count create_person in

  (* Write with specified compression *)
  let start_write = Unix.gettimeofday () in
  let writer = Container_writer.create ~path:temp_path ~codec:person_codec ~compression () in
  Array.iter (fun person -> Container_writer.write writer person) people;
  Container_writer.close writer;
  let write_elapsed = Unix.gettimeofday () -. start_write in

  let file_size = (Unix.stat temp_path).st_size in
  let write_mb_per_sec = (float_of_int file_size /. write_elapsed) /. 1_000_000.0 in

  (* Read compressed file *)
  let start_read = Unix.gettimeofday () in
  let reader = Container_reader.open_file ~path:temp_path ~codec:person_codec () in
  let count_read = ref 0 in
  Container_reader.iter (fun _person -> incr count_read) reader;
  Container_reader.close reader;
  let read_elapsed = Unix.gettimeofday () -. start_read in
  let read_mb_per_sec = (float_of_int file_size /. read_elapsed) /. 1_000_000.0 in

  Printf.printf "Compression[%s]: %d records\n" compression count;
  (match null_size_opt with
   | Some null_size ->
       let compression_ratio = float_of_int null_size /. float_of_int file_size in
       Printf.printf "  Uncompressed: %d bytes\n" null_size;
       Printf.printf "  Compressed:   %d bytes (%.2fx compression)\n" file_size compression_ratio
   | None ->
       Printf.printf "  File size:    %d bytes\n" file_size);
  Printf.printf "  Write: %.6f seconds (%.2f MB/s)\n" write_elapsed write_mb_per_sec;
  Printf.printf "  Read:  %.6f seconds (%.2f MB/s)\n" read_elapsed read_mb_per_sec;

  Sys.remove temp_path;
  file_size

let compare_codecs count =
  Printf.printf "=== Compression Codec Comparison (%d records) ===\n\n" count;
  let null_size = benchmark_compression count "null" None in
  Printf.printf "\n";
  let _ = benchmark_compression count "deflate" (Some null_size) in
  ()

let () =
  Avro.init_codecs ();
  let count = if Array.length Sys.argv > 1 then int_of_string Sys.argv.(1) else 10000 in
  compare_codecs count
