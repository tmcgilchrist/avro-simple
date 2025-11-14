(** Deflate compression codec for Avro using the decompress library.

    This module provides deflate (zlib/RFC 1950) compression and decompression
    functionality for Avro block compression. It uses the [decompress] library
    with configurable compression levels. *)

(** The codec configuration type containing compression parameters. *)
type t = { level: int }

(** The name of this codec: "deflate" *)
val name : string

(** [create ?level ()] creates a new deflate codec instance.

    @param level The compression level (0-9), where 0 is no compression and 9 is
                 maximum compression. Default is 6, which provides a good balance
                 between compression ratio and speed. *)
val create : ?level:int -> unit -> t

(** [compress t data] compresses the given byte data using zlib/deflate compression.

    @param t The codec instance with compression configuration
    @param data The input bytes to compress
    @return The compressed bytes *)
val compress : t -> bytes -> bytes

(** [decompress t data] decompresses zlib/deflate compressed data.

    @param t The codec instance (compression level is ignored during decompression)
    @param data The compressed bytes to decompress
    @return The decompressed bytes
    @raise Failure if the data is malformed or cannot be decompressed *)
val decompress : t -> bytes -> bytes

(** [register ()] registers this codec with the global codec registry.

    This function is called automatically when the module is loaded, so manual
    registration is typically not necessary. *)
val register : unit -> unit
