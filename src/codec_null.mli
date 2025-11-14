(** Null compression codec (passthrough)

    This codec provides a no-op compression implementation that passes data
    through without any transformation. It is useful as a baseline codec when
    no compression is desired or for testing purposes.
*)

(** The type of the null codec state. Since this is a passthrough codec,
    no state is needed. *)
type t = unit

(** The codec name identifier. *)
val name : string

(** [create ()] creates a new null codec instance.

    @return A unit value representing the codec state. *)
val create : unit -> t

(** [compress codec data] passes through the data without compression.

    @param codec The codec instance (ignored)
    @param data The data to compress
    @return The original data unchanged *)
val compress : t -> bytes -> bytes

(** [decompress codec data] passes through the data without decompression.

    @param codec The codec instance (ignored)
    @param data The data to decompress
    @return The original data unchanged *)
val decompress : t -> bytes -> bytes

(** [register ()] registers this codec with the global codec registry.

    This function makes the null codec available for use by name through
    the codec registry system. *)
val register : unit -> unit
