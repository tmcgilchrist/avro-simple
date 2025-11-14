(** Tests for container file format *)

open Avro_simple

(* Ensure codecs are registered *)
let () = Avro.init_codecs ()

(* TODO Create API that takes bytes or bigstring *)
(* TODO Make this relative to the test directory not an absolute path. *)
let test_file = "/tmp/test_avro_container.avro"

(** Test writing and reading integers *)
let test_int_roundtrip () =
  (* Write data *)
  let writer = Container_writer.create ~path:test_file ~codec:Codec.int () in
  Container_writer.write writer 42;
  Container_writer.write writer 100;
  Container_writer.write writer (-50);
  Container_writer.close writer;

  (* Read data back *)
  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.int () in
  let values = Container_reader.fold (fun acc v -> v :: acc) [] reader in
  Container_reader.close reader;

  (* Verify *)
  Alcotest.(check (list int)) "roundtrip integers"
    [-50; 100; 42]  (* Reversed because we cons to front *)
    values

(** Test writing and reading strings *)
let test_string_roundtrip () =
  let writer = Container_writer.create ~path:test_file ~codec:Codec.string () in
  Container_writer.write writer "hello";
  Container_writer.write writer "world";
  Container_writer.write writer "avro";
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.string () in
  let values = Container_reader.fold (fun acc v -> v :: acc) [] reader in
  Container_reader.close reader;

  Alcotest.(check (list string)) "roundtrip strings"
    ["avro"; "world"; "hello"]
    values

(** Test block writing *)
let test_write_block () =
  let writer = Container_writer.create ~path:test_file ~codec:Codec.int () in
  Container_writer.write_block writer [| 1; 2; 3; 4; 5 |];
  Container_writer.write_block writer [| 10; 20; 30 |];
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.int () in
  let values = Container_reader.fold (fun acc v -> v :: acc) [] reader in
  Container_reader.close reader;

  Alcotest.(check (list int)) "block writing"
    [30; 20; 10; 5; 4; 3; 2; 1]
    values

(** Test array codec *)
let test_array_roundtrip () =
  let codec = Codec.array Codec.int in
  let writer = Container_writer.create ~path:test_file ~codec () in
  Container_writer.write writer [| 1; 2; 3 |];
  Container_writer.write writer [| 4; 5 |];
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec () in
  let values = Container_reader.fold (fun acc v -> v :: acc) [] reader in
  Container_reader.close reader;

  Alcotest.(check int) "first array length" 2 (Array.length (List.nth values 0));
  Alcotest.(check int) "second array length" 3 (Array.length (List.nth values 1))

(** Test record codec *)
let test_record_roundtrip () =
  let type_name = Type_name.simple "Person" in
  let codec =
    Codec.record type_name (fun name age -> (name, age))
    |> Codec.field "name" Codec.string fst
    |> Codec.field "age" Codec.int snd
    |> Codec.finish
  in

  let writer = Container_writer.create ~path:test_file ~codec () in
  Container_writer.write writer ("Alice", 30);
  Container_writer.write writer ("Bob", 25);
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec () in
  let values = Container_reader.fold (fun acc v -> v :: acc) [] reader in
  Container_reader.close reader;

  Alcotest.(check (list (pair string int))) "record roundtrip"
    [("Bob", 25); ("Alice", 30)]
    values

(** Test iteration *)
let test_iter () =
  let writer = Container_writer.create ~path:test_file ~codec:Codec.int () in
  for i = 1 to 10 do
    Container_writer.write writer i
  done;
  Container_writer.close writer;

  let sum = ref 0 in
  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.int () in
  Container_reader.iter (fun v -> sum := !sum + v) reader;
  Container_reader.close reader;

  Alcotest.(check int) "sum via iter" 55 !sum

(** Test sequence *)
let test_sequence () =
  let writer = Container_writer.create ~path:test_file ~codec:Codec.int () in
  List.iter (Container_writer.write writer) [1; 2; 3; 4; 5];
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.int () in
  let seq = Container_reader.to_seq reader in
  let values = List.of_seq seq in
  Container_reader.close reader;

  Alcotest.(check (list int)) "sequence" [1; 2; 3; 4; 5] values

(** Test metadata *)
let test_metadata () =
  let metadata = [("author", "test"); ("version", "1.0")] in
  let writer = Container_writer.create ~path:test_file ~codec:Codec.int
    ~metadata () in
  Container_writer.write writer 42;
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.int () in
  let meta = Container_reader.metadata reader in
  Container_reader.close reader;

  Alcotest.(check (option string)) "author metadata"
    (Some "test") (List.assoc_opt "author" meta);
  Alcotest.(check (option string)) "version metadata"
    (Some "1.0") (List.assoc_opt "version" meta)

(** Test large dataset *)
let test_large_dataset () =
  let n = 10000 in
  let writer = Container_writer.create ~path:test_file ~codec:Codec.int
    ~sync_interval:100 () in
  for i = 0 to n - 1 do
    Container_writer.write writer i
  done;
  Container_writer.close writer;

  let reader = Container_reader.open_file ~path:test_file ~codec:Codec.int () in
  let count = Container_reader.fold (fun acc _ -> acc + 1) 0 reader in
  Container_reader.close reader;

  Alcotest.(check int) "large dataset count" n count

let () =
  let open Alcotest in
  run "Container Files" [
    "basic", [
      test_case "int roundtrip" `Quick test_int_roundtrip;
      test_case "string roundtrip" `Quick test_string_roundtrip;
      test_case "write block" `Quick test_write_block;
    ];
    "complex types", [
      test_case "array roundtrip" `Quick test_array_roundtrip;
      test_case "record roundtrip" `Quick test_record_roundtrip;
    ];
    "iteration", [
      test_case "iter" `Quick test_iter;
      test_case "sequence" `Quick test_sequence;
    ];
    "metadata", [
      test_case "custom metadata" `Quick test_metadata;
    ];
    "performance", [
      test_case "large dataset" `Quick test_large_dataset;
    ];
  ]
