type t = { level: int }

let name = "deflate"

let create ?(level=6) () = { level }

let compress t data =
  let input = Bigstringaf.of_string ~off:0 ~len:(Bytes.length data) (Bytes.to_string data) in
  let output_buffer = Buffer.create (Bytes.length data) in

  let w = De.Lz77.make_window ~bits:15 in
  let q = De.Queue.create 0x1000 in
  let o = De.bigstring_create De.io_buffer_size in

  let encoder = Zl.Def.encoder ~q ~w ~level:t.level `Manual `Manual in
  let encoder = Zl.Def.dst encoder o 0 De.io_buffer_size in

  let rec compress_loop encoder pos =
    match Zl.Def.encode encoder with
    | `Await encoder ->
        let len = min 0x1000 (Bigstringaf.length input - pos) in
        if len = 0 then
          compress_loop (Zl.Def.src encoder Bigstringaf.empty 0 0) pos
        else
          compress_loop (Zl.Def.src encoder input pos len) (pos + len)
    | `Flush encoder ->
        let len = De.io_buffer_size - Zl.Def.dst_rem encoder in
        let str = Bigstringaf.substring o ~off:0 ~len in
        Buffer.add_string output_buffer str;
        compress_loop (Zl.Def.dst encoder o 0 De.io_buffer_size) pos
    | `End encoder ->
        let len = De.io_buffer_size - Zl.Def.dst_rem encoder in
        if len > 0 then
          Buffer.add_string output_buffer (Bigstringaf.substring o ~off:0 ~len);
        Buffer.contents output_buffer
  in

  let compressed_str = compress_loop encoder 0 in
  Bytes.of_string compressed_str

let decompress _t data =
  let input = Bigstringaf.of_string ~off:0 ~len:(Bytes.length data) (Bytes.to_string data) in
  let output_buffer = Buffer.create (Bytes.length data * 2) in

  let o = De.bigstring_create De.io_buffer_size in
  let allocate bits = De.make_window ~bits in

  let decoder = Zl.Inf.decoder `Manual ~o ~allocate in

  let rec decompress_loop decoder pos =
    match Zl.Inf.decode decoder with
    | `Await decoder ->
        let len = min 0x1000 (Bigstringaf.length input - pos) in
        if len = 0 then
          decompress_loop (Zl.Inf.src decoder Bigstringaf.empty 0 0) pos
        else
          decompress_loop (Zl.Inf.src decoder input pos len) (pos + len)
    | `Flush decoder ->
        let len = De.io_buffer_size - Zl.Inf.dst_rem decoder in
        let str = Bigstringaf.substring o ~off:0 ~len in
        Buffer.add_string output_buffer str;
        decompress_loop (Zl.Inf.flush decoder) pos
    | `End decoder ->
        let len = De.io_buffer_size - Zl.Inf.dst_rem decoder in
        if len > 0 then
          Buffer.add_string output_buffer (Bigstringaf.substring o ~off:0 ~len);
        Buffer.contents output_buffer
    | `Malformed err ->
        failwith ("Deflate decompression error: " ^ err)
  in

  let decompressed_str = decompress_loop decoder 0 in
  Bytes.of_string decompressed_str

let register () =
  Codec_registry.register name (module struct
    type nonrec t = t
    let name = name
    let create () = create ()
    let compress = compress
    let decompress = decompress
  end : Codec_registry.CODEC)

let () = register ()
