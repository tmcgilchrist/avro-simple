(** Binary output encoding for Avro *)

(** The output encoder type that wraps a buffer for writing binary data *)
type t

(** [create ()] creates a new output encoder with a fresh buffer *)
val create : unit -> t

(** [of_buffer buffer] creates an output encoder from an existing buffer *)
val of_buffer : Buffer.t -> t

(** [contents t] returns the string contents of the encoder's buffer *)
val contents : t -> string

(** [to_bytes t] returns the byte contents of the encoder's buffer *)
val to_bytes : t -> bytes

(** [zigzag32 n] encodes a signed integer using zigzag encoding.
    Zigzag encoding maps signed integers to unsigned integers so that
    numbers with small absolute values have small encoded values. *)
val zigzag32 : int -> int32

(** [zigzag64 n] encodes a signed 64-bit integer using zigzag encoding.
    Zigzag encoding maps signed integers to unsigned integers so that
    numbers with small absolute values have small encoded values. *)
val zigzag64 : int64 -> int64

(** [write_long t n] writes a 64-bit integer to the encoder using
    variable-length encoding with zigzag encoding for signed values *)
val write_long : t -> int64 -> unit

(** [write_int t n] writes an integer to the encoder using
    variable-length encoding with zigzag encoding for signed values *)
val write_int : t -> int -> unit

(** [write_null t ()] writes a null value (no-op in binary encoding) *)
val write_null : t -> unit -> unit

(** [write_boolean t b] writes a boolean value as a single byte *)
val write_boolean : t -> bool -> unit

(** [write_float t f] writes a 32-bit floating point value in little-endian byte order *)
val write_float : t -> float -> unit

(** [write_double t f] writes a 64-bit floating point value in little-endian byte order *)
val write_double : t -> float -> unit

(** [write_bytes t bytes] writes a byte sequence with length prefix *)
val write_bytes : t -> bytes -> unit

(** [write_string t str] writes a string with length prefix *)
val write_string : t -> string -> unit

(** [write_fixed t bytes] writes fixed-length bytes without a length prefix *)
val write_fixed : t -> bytes -> unit
