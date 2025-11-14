(** Registry for compression codecs

    This module provides a pluggable registry for compression codecs used in
    Avro container files. Codecs can be built-in (null, deflate, zstandard, snappy)
    or custom user-provided implementations.

    {1 Built-in Codecs}

    The following codecs are available by default:

    - ["null"] - No compression (always available)
    - ["deflate"] - DEFLATE/GZIP compression using decompress library (always available)
    - ["zstandard"] - Zstandard compression (available if [zstd] package installed)
    - ["snappy"] - Snappy compression (available if [snappy] package installed)

    {1 Usage Example}

    {[
      (* List available codecs *)
      let codecs = Codec_registry.list () in
      List.iter print_endline codecs;

      (* Check if a codec is available *)
      match Codec_registry.get "zstandard" with
      | Some (module C : Codec_registry.CODEC) ->
          let compressor = C.create () in
          let compressed = C.compress compressor data in
          ...
      | None ->
          Printf.printf "zstandard codec not available\n"
    ]}

    {1 Custom Codecs}

    You can register your own compression codec:

    {[
      module My_LZ4_Codec = struct
        type t = unit
        let name = "lz4"
        let create () = ()
        let compress () data =
          (* Your LZ4 compression implementation *)
          Lz4.compress data
        let decompress () data =
          (* Your LZ4 decompression implementation *)
          Lz4.decompress data
      end

      (* Register the codec *)
      let () =
        Codec_registry.register "lz4" (module My_LZ4_Codec)

      (* Now it can be used in container files *)
      let writer = Container_writer.create ~compression:"lz4" codec path in
      ...
    ]}
*)

(** Module type for compression codecs *)
module type CODEC = sig
  (** Type representing codec state/configuration *)
  type t

  (** Codec name as it appears in Avro container file metadata *)
  val name : string

  (** Create a new codec instance with default configuration *)
  val create : unit -> t

  (** Compress data
      @param t codec instance
      @param data uncompressed data
      @return compressed data
      @raise Failure if compression fails
  *)
  val compress : t -> bytes -> bytes

  (** Decompress data
      @param t codec instance
      @param data compressed data
      @return decompressed data
      @raise Failure if decompression fails
  *)
  val decompress : t -> bytes -> bytes
end

(** Register a compression codec

    @param name codec identifier (e.g., "zstandard", "lz4")
    @param codec first-class module implementing the {!CODEC} signature

    Note: If a codec with the same name already exists, it will be replaced.
    This allows overriding built-in codecs with custom implementations.
*)
val register : string -> (module CODEC) -> unit

(** Get a registered codec by name

    @param name codec identifier
    @return [Some codec] if found, [None] otherwise
*)
val get : string -> (module CODEC) option

(** List all registered codec names

    @return list of codec identifiers
*)
val list : unit -> string list
