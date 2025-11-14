type t = unit

let name = "null"

let create () = ()

let compress () data = data

let decompress () data = data

let register () =
  Codec_registry.register name (module struct
    type nonrec t = t
    let name = name
    let create = create
    let compress = compress
    let decompress = decompress
  end : Codec_registry.CODEC)

let () = register ()
