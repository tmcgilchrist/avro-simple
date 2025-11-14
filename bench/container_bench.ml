(** Container file benchmarks *)

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

let create_person i =
  {
    name = Printf.sprintf "Person_%d" i;
    age = 20 + (i mod 60);
    email = (if i mod 3 = 0 then Some (Printf.sprintf "person%d@example.com" i) else None);
    phone_numbers = Array.init (1 + i mod 3) (fun j -> Printf.sprintf "+1-555-%04d" (i * 10 + j));
  }

let benchmark_write count compression =
  let temp_path = Printf.sprintf "test_container_bench_%s.avro" compression in
  let people = Array.init count create_person in

  let start_time = Unix.gettimeofday () in
  let writer = Container_writer.create ~path:temp_path ~codec:person_codec ~compression () in
  Array.iter (fun person -> Container_writer.write writer person) people;
  Container_writer.close writer;
  let elapsed = Unix.gettimeofday () -. start_time in

  let file_size = (Unix.stat temp_path).st_size in
  let mb_per_sec = (float_of_int file_size /. elapsed) /. 1_000_000.0 in

  Printf.printf "Container[%s]: Wrote %d records in %.6f seconds (%.2f MB/s, %d bytes)\n"
    compression count elapsed mb_per_sec file_size;

  temp_path

let benchmark_read path compression _count =
  let start_time = Unix.gettimeofday () in
  let reader = Container_reader.open_file ~path ~codec:person_codec () in
  let count_read = ref 0 in
  Container_reader.iter (fun _person -> incr count_read) reader;
  Container_reader.close reader;
  let elapsed = Unix.gettimeofday () -. start_time in

  let file_size = (Unix.stat path).st_size in
  let mb_per_sec = (float_of_int file_size /. elapsed) /. 1_000_000.0 in

  Printf.printf "Container[%s]: Read %d records in %.6f seconds (%.2f MB/s, %d bytes)\n"
    compression !count_read elapsed mb_per_sec file_size

let benchmark_container count compression =
  let temp_path = benchmark_write count compression in
  benchmark_read temp_path compression count;
  Sys.remove temp_path

let () =
  Avro.init_codecs ();
  let operation = if Array.length Sys.argv > 1 then Sys.argv.(1) else "container" in
  let count = if Array.length Sys.argv > 2 then int_of_string Sys.argv.(2) else 10000 in
  let compression = if Array.length Sys.argv > 3 then Sys.argv.(3) else "null" in

  match operation with
  | "container" | "write" | "read" -> benchmark_container count compression
  | _ ->
      Printf.eprintf "Usage: %s [container] [count] [compression]\n" Sys.argv.(0);
      Printf.eprintf "  compression: null or deflate (default: null)\n";
      exit 1
