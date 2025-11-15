(** Main entry point for the Avro library *)

module Schema = Schema
module Type_name = Type_name
module Codec = Codec
module Output = Output
module Input = Input
module Value = Value
module Decoder = Decoder
module Resolution = Resolution
module Fingerprint = Fingerprint
module Container_writer = Container_writer
module Container_reader = Container_reader
module Codec_registry = Codec_registry
module Schema_json = Schema_json
module Logical = Logical

(* Initialize compression codecs *)
let init_codecs () =
  (* Register built-in codecs *)
  Codec_null.register ();
  Codec_deflate.register ()

(* TODO We call this function directly in some places, is that necessary now with this module level let binding? *)
(* Auto-initialize on module load *)
let () = init_codecs ()
