type source =
  | File of in_channel
  | Bytes of bytes * int ref  (* bytes data and current position *)

type 'a t = {
  source: source;
  codec: 'a Codec.t; [@warning "-69"]
  writer_schema: Schema.t;
  compression: string;
  decompress: bytes -> bytes;
  sync_marker: bytes;
  metadata_map: (string * string) list;
  decoder: Input.t -> 'a;
}

let read_exact_from_source source n =
  match source with
  | File channel ->
      let bytes = Bytes.create n in
      really_input channel bytes 0 n;
      bytes
  | Bytes (data, pos_ref) ->
      let pos = !pos_ref in
      if pos + n > Bytes.length data then
        failwith "Unexpected end of bytes";
      let result = Bytes.sub data pos n in
      pos_ref := pos + n;
      result

let expected_magic = Bytes.of_string "Obj\x01"

let read_header_from_source source =
  let magic = read_exact_from_source source 4 in
  if magic <> expected_magic then
    failwith "Invalid Avro container file: bad magic bytes";

  (* Read metadata - we need to be able to read variable-length data *)
  let metadata_bytes = match source with
    | File channel ->
        let metadata_buffer = Bytes.create 8192 in
        let n = input channel metadata_buffer 0 8192 in
        Bytes.sub metadata_buffer 0 n
    | Bytes (data, pos_ref) ->
        (* Read enough bytes for metadata parsing *)
        let start_pos = !pos_ref in
        let remaining = Bytes.length data - start_pos in
        let read_size = min remaining 8192 in
        Bytes.sub data start_pos read_size
  in

  let inp = Input.of_bytes metadata_bytes in

  let rec read_map acc =
    let count = Input.read_long inp in
    if count = 0L then List.rev acc
    else if count < 0L then
      failwith "Negative map block count not supported"
    else
      let items = List.init (Int64.to_int count) (fun _ ->
        let key = Input.read_string inp in
        let value_bytes = Input.read_bytes inp in
        (key, Bytes.to_string value_bytes)
      ) in
      read_map (List.rev_append items acc)
  in
  let metadata_map = read_map [] in

  let metadata_size = Input.position inp in

  (* Seek/advance past the metadata *)
  (match source with
   | File channel -> seek_in channel (4 + metadata_size)
   | Bytes (_, pos_ref) -> pos_ref := 4 + metadata_size);

  let sync_marker = read_exact_from_source source 16 in

  (metadata_map, sync_marker)

let of_source source ~(codec : 'a Codec.t) () : 'a t =
  let (metadata_map, sync_marker) = read_header_from_source source in

  let writer_schema =
    match List.assoc_opt "avro.schema" metadata_map with
    | Some json ->
        begin match Schema_json.of_string json with
        | Ok schema -> schema
        | Error err ->
            failwith (Printf.sprintf "Failed to parse writer schema: %s" err)
        end
    | None -> failwith "Missing avro.schema in metadata"
  in

  let compression =
    match List.assoc_opt "avro.codec" metadata_map with
    | Some codec_name -> codec_name
    | None -> "null"
  in

  let decompress =
    match Codec_registry.get compression with
    | Some (module C : Codec_registry.CODEC) ->
        let decompressor = C.create () in
        C.decompress decompressor
    | None ->
        failwith (Printf.sprintf "Unknown compression codec: %s" compression)
  in

  let decoder = codec.Codec.decode in

  {
    source;
    codec;
    writer_schema;
    compression;
    decompress;
    sync_marker;
    metadata_map;
    decoder;
  }

let open_file ~path ~(codec : 'a Codec.t) () : 'a t =
  let channel = open_in_bin path in
  try
    of_source (File channel) ~codec ()
  with e ->
    close_in channel;
    raise e

let of_bytes data ~(codec : 'a Codec.t) () : 'a t =
  of_source (Bytes (data, ref 0)) ~codec ()

let writer_schema t = t.writer_schema

let codec_name t = t.compression

let metadata t = t.metadata_map

let read_long_from_source source =
  let unzigzag n =
    Int64.(logxor (shift_right_logical n 1) (neg (logand n 1L)))
  in
  let rec loop acc shift =
    let byte = match source with
      | File channel -> input_byte channel
      | Bytes (data, pos_ref) ->
          let pos = !pos_ref in
          if pos >= Bytes.length data then
            raise End_of_file;
          let byte_val = Char.code (Bytes.get data pos) in
          pos_ref := pos + 1;
          byte_val
    in
    let acc' = Int64.(logor acc (shift_left (of_int (byte land 0x7f)) shift)) in
    if byte land 0x80 = 0 then
      unzigzag acc'
    else
      loop acc' (shift + 7)
  in
  loop 0L 0

let read_block t =
  try
    let count = Int64.to_int (read_long_from_source t.source) in
    let byte_count = Int64.to_int (read_long_from_source t.source) in

    let compressed = read_exact_from_source t.source byte_count in

    let sync = read_exact_from_source t.source 16 in
    if sync <> t.sync_marker then
      failwith "Sync marker mismatch";

    let decompressed = t.decompress compressed in

    let inp = Input.of_bytes decompressed in
    let objects = Array.init count (fun _ -> t.decoder inp) in

    Some objects
  with
  | End_of_file -> None
  | Sys_error _ -> None

let iter f t =
  let rec loop () =
    match read_block t with
    | None -> ()
    | Some objects ->
        Array.iter f objects;
        (loop[@tailcall]) ()
  in
  loop ()

let fold f acc t =
  let rec loop acc =
    match read_block t with
    | None -> acc
    | Some objects ->
        let acc' = Array.fold_left f acc objects in
        (loop[@tailcall]) acc'
  in
  loop acc

let to_seq t =
  let rec blocks () =
    match read_block t with
    | None -> Seq.Nil
    | Some objects -> Seq.Cons (objects, blocks)
  in
  Seq.flat_map Array.to_seq blocks

let iter_blocks f t =
  let rec loop () =
    match read_block t with
    | None -> ()
    | Some objects ->
        f objects;
        (loop[@tailcall]) ()
  in
  loop ()

let open_at_offset ~path ~codec ~offset =
  let t = open_file ~path ~codec () in
  (match t.source with
   | File channel -> seek_in channel offset
   | Bytes _ -> failwith "open_at_offset not supported for bytes-based readers");
  t

let close t =
  match t.source with
  | File channel -> close_in channel
  | Bytes _ -> ()  (* Nothing to close for in-memory bytes *)
