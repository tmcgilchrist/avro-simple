(** Binary input decoding for Avro

    This module provides functionality for decoding Avro binary format data.
    It maintains a cursor position within the input buffer and provides
    functions to read various Avro primitive types. *)

(** The input decoder type, containing the binary data and current position. *)
type t

(** [of_bytes data] creates a new input decoder from a bytes sequence.
    The position is initialized to 0. *)
val of_bytes : bytes -> t

(** [of_string str] creates a new input decoder from a string.
    The string is converted to bytes and the position is initialized to 0. *)
val of_string : string -> t

(** [position t] returns the current position in the input buffer. *)
val position : t -> int

(** [remaining t] returns the number of bytes remaining in the input buffer
    from the current position. *)
val remaining : t -> int

(** [at_end t] returns true if the current position is at or beyond the end
    of the input buffer. *)
val at_end : t -> bool

(** Exception raised when attempting to read beyond the end of the input. *)
exception End_of_input

(** [unzigzag32 n] decodes a zigzag-encoded 32-bit integer.
    Zigzag encoding maps signed integers to unsigned integers so that numbers
    with small absolute values have small encoded values. *)
val unzigzag32 : int32 -> int

(** [unzigzag64 n] decodes a zigzag-encoded 64-bit integer.
    Zigzag encoding maps signed integers to unsigned integers so that numbers
    with small absolute values have small encoded values. *)
val unzigzag64 : int64 -> int64

(** [read_long t] reads a variable-length encoded long (64-bit integer) from
    the input buffer. The value is zigzag-decoded.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_long : t -> int64

(** [read_int t] reads a variable-length encoded integer from the input buffer.
    The value is zigzag-decoded and converted to a native int.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_int : t -> int

(** [read_null t] reads a null value (which has no data representation). *)
val read_null : t -> unit

(** [read_boolean t] reads a boolean value from the input buffer.
    The boolean is encoded as a single byte: 0 for false, non-zero for true.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_boolean : t -> bool

(** [read_float t] reads a 32-bit IEEE 754 floating-point number from the
    input buffer in little-endian byte order.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_float : t -> float

(** [read_double t] reads a 64-bit IEEE 754 floating-point number from the
    input buffer in little-endian byte order.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_double : t -> float

(** [read_bytes t] reads a variable-length byte sequence from the input buffer.
    The length is read first as a variable-length integer, followed by that
    many bytes of data.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_bytes : t -> bytes

(** [read_string t] reads a variable-length UTF-8 string from the input buffer.
    The length is read first as a variable-length integer, followed by that
    many bytes of UTF-8 encoded data.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_string : t -> string

(** [read_fixed t size] reads exactly [size] bytes from the input buffer.
    This is used for Avro's fixed-length byte arrays.
    @raise End_of_input if there is not enough data in the buffer. *)
val read_fixed : t -> int -> bytes
