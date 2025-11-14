(* Benchmark using ocaml-avro (code generation approach)
   This uses Simon Cruanes' ocaml-avro library which generates code from schemas.
   Similar to Java's SpecificRecord approach. *)

let create_person i =
  let open Person_schema in
  {
    name = Printf.sprintf "Person_%d" i;
    age = 20 + (i mod 60);
    email = (if i mod 3 = 0 then Some (Printf.sprintf "person%d@example.com" i) else None);
    phone_numbers = Array.init (1 + i mod 3) (fun j -> Printf.sprintf "+1-555-%04d" (i * 10 + j));
  }

let benchmark_encode count =
  let people = Array.init count create_person in

  let start_time = Unix.gettimeofday () in
  let encoded = Array.map (fun person ->
    let buf = Buffer.create 256 in
    let out = Avro.Output.of_buffer buf in
    Person_schema.write out person;
    Buffer.contents buf
  ) people in
  let elapsed = Unix.gettimeofday () -. start_time in

  let total_bytes = Array.fold_left (fun acc s -> acc + String.length s) 0 encoded in
  let mb_per_sec = (float_of_int total_bytes /. elapsed) /. 1_000_000.0 in

  Printf.printf "Encoded %d records in %f seconds (%.2f MB/s, %d bytes)\n"
    count elapsed mb_per_sec total_bytes

let benchmark_decode count =
  let people = Array.init count create_person in

  (* Encode first *)
  let encoded = Array.map (fun person ->
    let buf = Buffer.create 256 in
    let out = Avro.Output.of_buffer buf in
    Person_schema.write out person;
    Buffer.contents buf
  ) people in

  let total_bytes = Array.fold_left (fun acc s -> acc + String.length s) 0 encoded in

  (* Benchmark decode *)
  let start_time = Unix.gettimeofday () in
  Array.iter (fun data ->
    let input = Avro.Input.of_string data in
    let _ = Person_schema.read input in
    ()
  ) encoded;
  let elapsed = Unix.gettimeofday () -. start_time in

  let mb_per_sec = (float_of_int total_bytes /. elapsed) /. 1_000_000.0 in

  Printf.printf "Decoded %d records in %f seconds (%.2f MB/s, %d bytes)\n"
    count elapsed mb_per_sec total_bytes

let () =
  let operation = if Array.length Sys.argv > 1 then Sys.argv.(1) else "encode" in
  let count = if Array.length Sys.argv > 2 then int_of_string Sys.argv.(2) else 10000 in

  match operation with
  | "encode" -> benchmark_encode count
  | "decode" -> benchmark_decode count
  | _ ->
      Printf.eprintf "Usage: %s [encode|decode] [count]\n" Sys.argv.(0);
      Printf.eprintf "Note: container operations not supported by ocaml-avro codegen\n";
      exit 1
