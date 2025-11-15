(** Large file streaming example

    This example demonstrates memory-efficient processing of large Avro
    container files using lazy sequences and iterators.
*)

open Avro_simple

type event = {
  timestamp: int64;
  user_id: int;
  event_type: string;
  value: int;
}

let () =
  (* Initialize compression codecs *)
  Avro.init_codecs ();

  Printf.printf "=== Large File Streaming Example ===\n\n";

  let temp_dir = Filename.get_temp_dir_name () in

  (* Define codec *)
  let event_codec =
    Codec.record (Type_name.simple "Event")
      (fun timestamp user_id event_type value ->
        { timestamp; user_id; event_type; value })
    |> Codec.field "timestamp" Codec.long (fun e -> e.timestamp)
    |> Codec.field "user_id" Codec.int (fun e -> e.user_id)
    |> Codec.field "event_type" Codec.string (fun e -> e.event_type)
    |> Codec.field "value" Codec.int (fun e -> e.value)
    |> Codec.finish
  in

  (* Example 1: Create a large file *)
  Printf.printf "1. Creating large test file (100K records):\n";

  let filename = Filename.concat temp_dir "large_events.avro" in
  let writer = Container_writer.create
    ~path:filename
    ~codec:event_codec
    ~compression:"deflate"
    ~sync_interval:1000  (* Flush every 1000 records = smaller blocks *)
    ()
  in

  let start_time = Unix.gettimeofday () in

  (* Generate and write 100,000 events *)
  for i = 0 to 99_999 do
    let event = {
      timestamp = Int64.of_int (1609459200 + i);
      user_id = i mod 1000;
      event_type = if i mod 3 = 0 then "click"
                   else if i mod 3 = 1 then "view"
                   else "purchase";
      value = i mod 100;
    } in
    Container_writer.write writer event;

    if (i + 1) mod 10000 = 0 then
      Printf.printf "   Written %d records...\n%!" (i + 1)
  done;

  Container_writer.close writer;

  let write_time = Unix.gettimeofday () -. start_time in
  let file_size = (Unix.stat filename).Unix.st_size in

  Printf.printf "   Complete! Wrote 100K records in %.2fs\n" write_time;
  Printf.printf "   File size: %.2f MB\n" (float_of_int file_size /. 1_048_576.0);
  Printf.printf "   Throughput: %.0f records/sec\n\n"
    (100_000.0 /. write_time);

  (* Example 2: Streaming with lazy sequences *)
  Printf.printf "2. Streaming with lazy sequences (filter + take):\n";

  let reader = Container_reader.open_file ~path:filename ~codec:event_codec () in

  let start_time = Unix.gettimeofday () in

  (* Lazy: Only process what we need *)
  let first_10_purchases =
    Container_reader.to_seq reader
    |> Seq.filter (fun e -> e.event_type = "purchase")
    |> Seq.take 10
    |> List.of_seq
  in

  Container_reader.close reader;

  let lazy_time = Unix.gettimeofday () -. start_time in

  Printf.printf "   Found first 10 purchases in %.4fs\n" lazy_time;
  Printf.printf "   First purchase: user_id=%d, timestamp=%Ld\n"
    (List.hd first_10_purchases).user_id
    (List.hd first_10_purchases).timestamp;
  Printf.printf "   Memory efficient: Only scanned until 10 purchases found\n\n";

  (* Example 3: Full scan with iterator (memory efficient) *)
  Printf.printf "3. Full scan with iterator (aggregation):\n";

  let reader = Container_reader.open_file ~path:filename ~codec:event_codec () in

  let start_time = Unix.gettimeofday () in
  let count = ref 0 in

  Container_reader.iter (fun _event ->
    incr count;
    (* Process each event - memory is freed after each block *)
  ) reader;

  Container_reader.close reader;

  let iter_time = Unix.gettimeofday () -. start_time in

  Printf.printf "   Scanned %d records in %.3fs\n" !count iter_time;
  Printf.printf "   Throughput: %.0f records/sec\n"
    (float_of_int !count /. iter_time);
  Printf.printf "   Peak memory: ~1-2 MB (one block at a time)\n\n";

  (* Example 4: Aggregation with fold *)
  Printf.printf "4. Aggregation with fold (count by type):\n";

  let reader = Container_reader.open_file ~path:filename ~codec:event_codec () in

  let start_time = Unix.gettimeofday () in

  let counts = Container_reader.fold (fun acc event ->
    let count =
      try List.assoc event.event_type acc
      with Not_found -> 0
    in
    (event.event_type, count + 1) :: List.remove_assoc event.event_type acc
  ) [] reader in

  Container_reader.close reader;

  let fold_time = Unix.gettimeofday () -. start_time in

  Printf.printf "   Event counts in %.3fs:\n" fold_time;
  List.iter (fun (event_type, count) ->
    Printf.printf "     %s: %d\n" event_type count
  ) counts;
  Printf.printf "\n";

  (* Example 5: Sequential processing (transform and write) *)
  Printf.printf "5. Transform and write (streaming pipeline):\n";

  let output_filename = Filename.concat temp_dir "filtered_events.avro" in

  let reader = Container_reader.open_file ~path:filename ~codec:event_codec () in
  let writer = Container_writer.create
    ~path:output_filename
    ~codec:event_codec
    ~compression:"deflate"
    ()
  in

  let start_time = Unix.gettimeofday () in
  let written = ref 0 in

  (* Stream: read -> filter -> transform -> write *)
  Container_reader.to_seq reader
  |> Seq.filter (fun e -> e.value > 50)
  |> Seq.map (fun e -> { e with value = e.value * 2 })
  |> Seq.iter (fun e ->
      Container_writer.write writer e;
      incr written
    );

  Container_reader.close reader;
  Container_writer.close writer;

  let pipeline_time = Unix.gettimeofday () -. start_time in

  Printf.printf "   Filtered and transformed %d events in %.3fs\n"
    !written pipeline_time;
  Printf.printf "   Output file size: %.2f MB\n"
    (float_of_int (Unix.stat output_filename).Unix.st_size /. 1_048_576.0);
  Printf.printf "   Memory efficient: Streaming pipeline (no intermediate storage)\n\n";

  (* Example 6: Block-level processing *)
  Printf.printf "6. Block-level processing (advanced):\n";

  let reader = Container_reader.open_file ~path:filename ~codec:event_codec () in

  let start_time = Unix.gettimeofday () in
  let block_count = ref 0 in
  let total_records = ref 0 in

  Container_reader.iter_blocks (fun block ->
    incr block_count;
    total_records := !total_records + Array.length block;

    if !block_count <= 3 then
      Printf.printf "   Block %d: %d records\n" !block_count (Array.length block)
  ) reader;

  Container_reader.close reader;

  let block_time = Unix.gettimeofday () -. start_time in

  Printf.printf "   ... (showing first 3 blocks)\n";
  Printf.printf "   Total: %d blocks, %d records in %.3fs\n"
    !block_count !total_records block_time;
  Printf.printf "   Average block size: %d records\n\n"
    (!total_records / !block_count);

  (* Example 7: Memory usage demonstration *)
  Printf.printf "7. Memory usage analysis:\n";

  let reader = Container_reader.open_file ~path:filename ~codec:event_codec () in

  (* Process first block to see memory characteristics *)
  let first_block = Container_reader.read_block reader in

  begin match first_block with
  | Some block ->
      let block_size = Array.length block in
      Printf.printf "   First block: %d records\n" block_size;
      Printf.printf "   Estimated memory per block: ~%.0f KB\n"
        (float_of_int block_size *. 64.0 /. 1024.0);  (* ~64 bytes per event *)
      Printf.printf "   Total blocks: ~%d\n" (100_000 / block_size);
      Printf.printf "   Peak memory: 1 block at a time (not whole file)\n"
  | None ->
      Printf.printf "   No blocks found\n"
  end;

  Container_reader.close reader;
  Printf.printf "\n";

  (* Cleanup *)
  Printf.printf "8. Cleaning up:\n";
  List.iter (fun file ->
    if Sys.file_exists file then begin
      Sys.remove file;
      Printf.printf "   Removed %s\n" (Filename.basename file)
    end
  ) [filename; output_filename];

  Printf.printf "\n=== Key Takeaways ===\n";
  Printf.printf "✓ Lazy sequences enable efficient filtering without loading full file\n";
  Printf.printf "✓ Iterator provides maximum throughput for full scans\n";
  Printf.printf "✓ Fold enables aggregation with constant memory overhead\n";
  Printf.printf "✓ Seq combinators create streaming pipelines\n";
  Printf.printf "✓ Block-level access for advanced use cases\n";
  Printf.printf "✓ Memory usage: O(block_size) not O(file_size)\n";
  Printf.printf "✓ Can process files larger than available RAM\n";
  Printf.printf "\n=== Done ===\n"
