(** Container file format example

    This example demonstrates Avro Object Container Files (OCF), which provide:
    - File header with metadata and schema
    - Compressed data blocks
    - Sync markers for parallel processing
    - Support for multiple compression codecs
*)

(* Type definition *)
type event = {
  timestamp: int64;
  user_id: int;
  event_type: string;
}

let () =
  (* Initialize compression codecs *)
  Avro_simple.Avro.init_codecs ();

  Printf.printf "=== Avro Container Files Example ===\n\n";

  (* Get platform-independent temporary directory *)
  let temp_dir = Filename.get_temp_dir_name () in

  (* Define a codec for our data *)
  let event_codec =
    Avro_simple.Codec.record (Avro_simple.Type_name.simple "Event")
      (fun timestamp user_id event_type -> ({ timestamp; user_id; event_type } : event))
    |> Avro_simple.Codec.field "timestamp" Avro_simple.Codec.long (fun (e : event) -> e.timestamp)
    |> Avro_simple.Codec.field "user_id" Avro_simple.Codec.int (fun (e : event) -> e.user_id)
    |> Avro_simple.Codec.field "event_type" Avro_simple.Codec.string (fun (e : event) -> e.event_type)
    |> Avro_simple.Codec.finish in

  (* Example 1: Writing a container file with null compression *)
  Printf.printf "1. Writing container file (null compression):\n";

  (* Use platform-independent temporary directory *)
  let filename_null = Filename.concat temp_dir "events_null.avro" in
  let writer = Avro_simple.Container_writer.create
    ~path:filename_null
    ~codec:event_codec
    ~compression:"null"
    () in

  (* Write some events *)
  let events = [
    { timestamp = 1609459200L; user_id = 1; event_type = "login" };
    { timestamp = 1609459201L; user_id = 1; event_type = "click" };
    { timestamp = 1609459202L; user_id = 2; event_type = "login" };
    { timestamp = 1609459203L; user_id = 2; event_type = "purchase" };
    { timestamp = 1609459204L; user_id = 3; event_type = "login" };
  ] in

  List.iter (fun event -> Avro_simple.Container_writer.write writer event) events;
  Avro_simple.Container_writer.close writer;

  Printf.printf "   Wrote %d events to %s\n" (List.length events) filename_null;

  (* Example 2: Reading the container file back *)
  Printf.printf "\n2. Reading container file:\n";

  let reader = Avro_simple.Container_reader.open_file
    ~path:filename_null
    ~codec:event_codec
    () in

  Printf.printf "   Codec: %s\n" (Avro_simple.Container_reader.codec_name reader);
  Printf.printf "   Events:\n";

  let count = ref 0 in
  Avro_simple.Container_reader.iter (fun event ->
    incr count;
    Printf.printf "     [%d] timestamp=%Ld, user=%d, type=%s\n"
      !count event.timestamp event.user_id event.event_type
  ) reader;

  Avro_simple.Container_reader.close reader;

  (* Example 3: Writing with deflate compression *)
  Printf.printf "\n3. Writing with deflate compression:\n";

  let filename_deflate = Filename.concat temp_dir "events_deflate.avro" in
  let writer_deflate = Avro_simple.Container_writer.create
    ~path:filename_deflate
    ~codec:event_codec
    ~compression:"deflate"
    () in

  (* Write more events *)
  let large_events = List.init 100 (fun i ->
    { timestamp = Int64.of_int (1609459200 + i);
      user_id = i mod 10;
      event_type = if i mod 2 = 0 then "click" else "view" }
  ) in

  List.iter (fun event -> Avro_simple.Container_writer.write writer_deflate event) large_events;
  Avro_simple.Container_writer.close writer_deflate;

  Printf.printf "   Wrote %d events with deflate compression\n" (List.length large_events);

  (* Compare file sizes *)
  let size_null = (Unix.stat filename_null).Unix.st_size in
  let size_deflate = (Unix.stat filename_deflate).Unix.st_size in
  Printf.printf "   File size (null): %d bytes\n" size_null;
  Printf.printf "   File size (deflate): %d bytes\n" size_deflate;
  Printf.printf "   Compression ratio: %.1f%%\n"
    (100.0 *. (1.0 -. float_of_int size_deflate /. float_of_int size_null));

  (* Example 4: Using fold to aggregate data *)
  Printf.printf "\n4. Aggregating data with fold:\n";

  let reader_deflate = Avro_simple.Container_reader.open_file
    ~path:filename_deflate
    ~codec:event_codec
    () in

  (* Count events by type *)
  let event_counts = Avro_simple.Container_reader.fold (fun acc event ->
    let count = try List.assoc event.event_type acc with Not_found -> 0 in
    (event.event_type, count + 1) :: List.remove_assoc event.event_type acc
  ) [] reader_deflate in

  Printf.printf "   Event type counts:\n";
  List.iter (fun (event_type, count) ->
    Printf.printf "     %s: %d\n" event_type count
  ) event_counts;

  Avro_simple.Container_reader.close reader_deflate;

  (* Example 5: Writing blocks explicitly for better control *)
  Printf.printf "\n5. Writing data in blocks:\n";

  let filename_blocks = Filename.concat temp_dir "events_blocks.avro" in
  let writer_blocks = Avro_simple.Container_writer.create
    ~path:filename_blocks
    ~codec:event_codec
    ~compression:"deflate"
    ~sync_interval:10  (* Force new block every 10 objects *)
    () in

  (* Write in batches *)
  let batch1 = Array.init 15 (fun i ->
    { timestamp = Int64.of_int i; user_id = i; event_type = "batch1" }
  ) in
  Avro_simple.Container_writer.write_block writer_blocks batch1;

  let batch2 = Array.init 15 (fun i ->
    { timestamp = Int64.of_int (i + 100); user_id = i + 100; event_type = "batch2" }
  ) in
  Avro_simple.Container_writer.write_block writer_blocks batch2;

  Avro_simple.Container_writer.close writer_blocks;
  Printf.printf "   Wrote 2 batches (15 events each) with explicit blocks\n";

  (* Example 6: Iterating over blocks *)
  Printf.printf "\n6. Reading blocks explicitly:\n";

  let reader_blocks = Avro_simple.Container_reader.open_file
    ~path:filename_blocks
    ~codec:event_codec
    () in

  let block_num = ref 0 in
  Avro_simple.Container_reader.iter_blocks (fun block ->
    incr block_num;
    Printf.printf "   Block %d: %d events\n" !block_num (Array.length block);
  ) reader_blocks;

  Avro_simple.Container_reader.close reader_blocks;

  (* Example 7: Using sequences for lazy processing *)
  Printf.printf "\n7. Lazy sequence processing:\n";

  let reader_seq = Avro_simple.Container_reader.open_file
    ~path:filename_deflate
    ~codec:event_codec
    () in

  (* Process only first 5 events using sequence *)
  let first_5 = Avro_simple.Container_reader.to_seq reader_seq
    |> Seq.take 5
    |> List.of_seq in

  Printf.printf "   First 5 events (lazy loaded):\n";
  List.iteri (fun i event ->
    Printf.printf "     [%d] user=%d, type=%s\n" (i+1) event.user_id event.event_type
  ) first_5;

  Avro_simple.Container_reader.close reader_seq;

  (* Example 8: Check available compression codecs *)
  Printf.printf "\n8. Available compression codecs:\n";

  let codecs = Avro_simple.Codec_registry.list () in
  List.iter (fun codec ->
    Printf.printf "   - %s\n" codec
  ) codecs;

  (* Example 9: Custom metadata *)
  Printf.printf "\n9. Writing with custom metadata:\n";

  let filename_metadata = Filename.concat temp_dir "events_metadata.avro" in
  let writer_metadata = Avro_simple.Container_writer.create
    ~path:filename_metadata
    ~codec:event_codec
    ~compression:"null"
    ~metadata:[
      ("created_by", "OCaml Avro Example");
      ("version", "1.0");
      ("purpose", "demonstration")
    ]
    () in

  Avro_simple.Container_writer.write writer_metadata { timestamp = 0L; user_id = 0; event_type = "test" };
  Avro_simple.Container_writer.close writer_metadata;

  (* Read metadata back *)
  let reader_metadata = Avro_simple.Container_reader.open_file
    ~path:filename_metadata
    ~codec:event_codec
    () in

  let metadata = Avro_simple.Container_reader.metadata reader_metadata in
  Printf.printf "   Metadata from file:\n";
  List.iter (fun (key, value) ->
    if not (String.starts_with ~prefix:"avro." key) then
      Printf.printf "     %s: %s\n" key value
  ) metadata;

  Avro_simple.Container_reader.close reader_metadata;

  (* Cleanup *)
  Printf.printf "\n10. Cleaning up temporary files:\n";
  List.iter (fun filename ->
    if Sys.file_exists filename then begin
      Sys.remove filename;
      Printf.printf "   Removed %s\n" filename
    end
  ) [filename_null; filename_deflate; filename_blocks; filename_metadata];

  Printf.printf "\n=== Container Files Key Takeaways ===\n";
  Printf.printf "✓ Container files include schema in header\n";
  Printf.printf "✓ Multiple compression codecs supported (null, deflate, snappy, zstd)\n";
  Printf.printf "✓ Sync markers enable parallel processing\n";
  Printf.printf "✓ Custom metadata can be stored\n";
  Printf.printf "✓ Iterator, fold, and sequence APIs available\n";
  Printf.printf "✓ Block-level writing for better control\n";
  Printf.printf "\n=== Done ===\n"
