type 'a t = {
  channel: in_channel;
  codec: 'a Codec.t; [@warning "-69"]
  writer_schema: Schema.t;
  compression: string;
  decompress: bytes -> bytes;
  sync_marker: bytes;
  metadata_map: (string * string) list;
  decoder: Input.t -> 'a;
}

let read_exact channel n =
  let bytes = Bytes.create n in
  really_input channel bytes 0 n;
  bytes

let read_header channel =
  let magic = read_exact channel 4 in
  if Bytes.to_string magic <> "Obj\x01" then
    failwith "Invalid Avro container file: bad magic bytes";

  let metadata_buffer = Bytes.create 8192 in
  let n = input channel metadata_buffer 0 8192 in
  let metadata_bytes = Bytes.sub metadata_buffer 0 n in
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
  seek_in channel (4 + metadata_size);

  let sync_marker = read_exact channel 16 in

  (metadata_map, sync_marker)

let open_file ~path ~(codec : 'a Codec.t) () : 'a t =
  let channel = open_in_bin path in

  try
    let (metadata_map, sync_marker) = read_header channel in

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
      channel;
      codec;
      writer_schema;
      compression;
      decompress;
      sync_marker;
      metadata_map;
      decoder;
    }
  with e ->
    close_in channel;
    raise e

let writer_schema t = t.writer_schema

let codec_name t = t.compression

let metadata t = t.metadata_map

let read_long_from_channel channel =
  let unzigzag n =
    Int64.(logxor (shift_right_logical n 1) (neg (logand n 1L)))
  in
  let rec loop acc shift =
    let byte = input_byte channel in
    let acc' = Int64.(logor acc (shift_left (of_int (byte land 0x7f)) shift)) in
    if byte land 0x80 = 0 then
      unzigzag acc'
    else
      loop acc' (shift + 7)
  in
  loop 0L 0

let read_block t =
  try
    let count = Int64.to_int (read_long_from_channel t.channel) in
    let byte_count = Int64.to_int (read_long_from_channel t.channel) in

    let compressed = read_exact t.channel byte_count in

    let sync = read_exact t.channel 16 in
    if sync <> t.sync_marker then
      failwith "Sync marker mismatch";

    let decompressed = t.decompress compressed in

    let inp = Input.of_bytes decompressed in
    let objects = List.init count (fun _ -> t.decoder inp) in

    Some (Array.of_list objects)
  with
  | End_of_file -> None
  | Sys_error _ -> None

(* TODO Some of these helpers could be re-written? *)
let iter f t =
  let rec loop () =
    match read_block t with
    | None -> ()
    | Some objects ->
        Array.iter f objects;
        loop ()
  in
  loop ()

let fold f acc t =
  let rec loop acc =
    match read_block t with
    | None -> acc
    | Some objects ->
        let acc' = Array.fold_left f acc objects in
        loop acc'
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
        loop ()
  in
  loop ()

(* TODO Is this function used? We should implement it or remove it *)
let seek_to_block _t _n =
  failwith "seek_to_block not implemented - requires block index"

let open_at_offset ~path ~codec ~offset =
  let t = open_file ~path ~codec () in
  seek_in t.channel offset;
  t

let close t =
  close_in t.channel
