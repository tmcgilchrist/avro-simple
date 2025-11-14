(** Generic Avro value representation.

    This module provides a generic value type that can represent any Avro data,
    along with utilities for working with these values. The generic value type
    is useful for dynamic scenarios where the schema is not known at compile time,
    or when you need a uniform representation across different Avro types.
*)

(** The type of generic Avro values.

    A value can represent any Avro data type defined in the Avro specification.
    Each variant corresponds to one of the Avro primitive or complex types:

    - [Null]: The null value
    - [Boolean of bool]: A boolean value (true or false)
    - [Int of int]: A 32-bit signed integer
    - [Long of int64]: A 64-bit signed integer
    - [Float of float]: A single-precision (32-bit) IEEE 754 floating-point number
    - [Double of float]: A double-precision (64-bit) IEEE 754 floating-point number
    - [Bytes of bytes]: A sequence of 8-bit unsigned bytes
    - [String of string]: A unicode character sequence
    - [Array of t array]: An array of values (all of the same schema)
    - [Map of (string * t) list]: A map from string keys to values
    - [Record of (string * t) list]: A record with named fields (field name -> value)
    - [Enum of int * string]: An enumeration value with its index and symbol
    - [Union of int * t]: A union value with the branch index and the actual value
    - [Fixed of bytes]: A fixed-length byte sequence
*)
type t =
  | Null
  | Boolean of bool
  | Int of int
  | Long of int64
  | Float of float
  | Double of float
  | Bytes of bytes
  | String of string
  | Array of t array
  | Map of (string * t) list
  | Record of (string * t) list
  | Enum of int * string
  | Union of int * t
  | Fixed of bytes

(** [equal v1 v2] tests structural equality between two Avro values.

    Two values are equal if they have the same variant constructor and their
    contents are equal. For complex types (arrays, maps, records), equality
    is tested recursively on all elements/fields.

    @param v1 The first value to compare
    @param v2 The second value to compare
    @return [true] if the values are structurally equal, [false] otherwise
*)
val equal : t -> t -> bool

(** [of_default default] converts a schema default value to a generic value.

    This function converts default values as specified in Avro schemas into
    the generic value representation. Note that for enum defaults, the index
    is set to 0 as a placeholder since the full list of symbols is not available
    in the default value representation.

    @param default A default value from a schema
    @return The corresponding generic value
*)
val of_default : Schema.default -> t

(** [to_string value] converts an Avro value to a human-readable string representation.

    This function is primarily intended for debugging and logging purposes.
    The output format is similar to JSON but includes type information for
    disambiguation (e.g., "L" suffix for longs, "f" suffix for floats).

    Examples:
    - [Null] is represented as ["null"]
    - [Boolean true] is represented as ["true"]
    - [Long 42L] is represented as ["42L"]
    - [Array [|Int 1; Int 2|]] is represented as ["[1, 2]"]
    - [Enum (0, "RED")] is represented as ["\"RED\"(0)"]

    @param value The value to convert
    @return A string representation of the value
*)
val to_string : t -> string
