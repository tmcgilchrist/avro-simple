type 'a t = {
  channel: out_channel;
  codec: 'a Codec.t;
  compression: string;
  compress: bytes -> bytes;
  sync_marker: bytes;
  sync_interval: int;
  mutable buffer: 'a list;
  mutable objects_written: int;
}

let generate_sync_marker () =
  Random.self_init ();
  let marker = Bytes.create 16 in
  for i = 0 to 15 do
    Bytes.set marker i (Char.chr (Random.int 256))
  done;
  marker

let write_header channel schema compression metadata sync_marker =
  (* Magic bytes: "Obj" followed by 0x01 *)
  output_string channel "Obj\x01";

  (* File metadata as map *)
  let meta_items =
    ("avro.schema", Fingerprint.to_canonical_json schema) ::
    ("avro.codec", compression) ::
    metadata
  in

  (* Write metadata map *)
  let out = Output.create () in
  Output.write_long out (Int64.of_int (List.length meta_items));
  List.iter (fun (key, value) ->
    Output.write_string out key;
    Output.write_bytes out (Bytes.of_string value)
  ) meta_items;
  Output.write_long out 0L; (* End of map *)
  output_bytes channel (Output.to_bytes out);

  (* Write sync marker *)
  output_bytes channel sync_marker

let create ~path ~codec ?(compression="null") ?(metadata=[]) ?(sync_interval=4000) () =
  let channel = open_out_bin path in
  let sync_marker = generate_sync_marker () in

  (* Get compression function *)
  let compress =
    match Codec_registry.get compression with
    | Some (module C : Codec_registry.CODEC) ->
        let compressor = C.create () in
        C.compress compressor
    | None ->
        failwith (Printf.sprintf "Unknown compression codec: %s" compression)
  in

  (* Write header *)
  write_header channel codec.Codec.schema compression metadata sync_marker;

  {
    channel;
    codec;
    compression;
    compress;
    sync_marker;
    sync_interval;
    buffer = [];
    objects_written = 0;
  }

let flush_block t =
  if t.buffer = [] then ()
  else begin
    let objects = List.rev t.buffer in
    let count = List.length objects in

    (* Serialize objects *)
    let out = Output.create () in
    List.iter (fun obj -> t.codec.Codec.encode obj out) objects;
    let serialized = Output.to_bytes out in

    (* Compress if needed *)
    let compressed = t.compress serialized in

    (* Write block header *)
    let block_out = Output.create () in
    Output.write_long block_out (Int64.of_int count);
    Output.write_long block_out (Int64.of_int (Bytes.length compressed));
    output_bytes t.channel (Output.to_bytes block_out);

    (* Write compressed data *)
    output_bytes t.channel compressed;

    (* Write sync marker *)
    output_bytes t.channel t.sync_marker;

    (* Clear buffer *)
    t.buffer <- [];
    t.objects_written <- t.objects_written + count
  end

let write t value =
  t.buffer <- value :: t.buffer;
  if List.length t.buffer >= t.sync_interval then
    flush_block t

let write_block t values =
  flush_block t; (* Flush any pending data first *)
  (* Buffer is maintained in reverse order, so build list by prepending from end *)
  let acc = ref [] in
  for i = 0 to Array.length values - 1 do
    acc := values.(i) :: !acc
  done;
  t.buffer <- !acc;
  flush_block t

let flush t =
  flush_block t;
  flush t.channel

let close t =
  flush_block t;
  close_out t.channel
