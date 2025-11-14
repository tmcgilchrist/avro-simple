open Avro_simple

let () =
  Printf.printf "Codec list: [%s]\n" (String.concat "; " (Codec_registry.list ()));
  Printf.printf "Number of codecs: %d\n" (List.length (Codec_registry.list ()));

  match Codec_registry.get "null" with
  | Some _ -> Printf.printf "null codec IS registered\n"
  | None -> Printf.printf "null codec NOT registered\n";

  match Codec_registry.get "deflate" with
  | Some _ -> Printf.printf "deflate codec IS registered\n"
  | None -> Printf.printf "deflate codec NOT registered\n"
