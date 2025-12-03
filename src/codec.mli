(** Codec combinators for encoding and decoding Avro data.

    This module provides the core functionality for building type-safe encoders
    and decoders for Avro data. A codec combines three things:

    - An Avro schema describing the data structure
    - An encoder that converts OCaml values to Avro binary format
    - A decoder that converts Avro binary format back to OCaml values

    {1 Overview}

    Codecs are built using a combinator-based approach. Primitive codecs like
    {!int}, {!string}, and {!boolean} can be composed using container codecs
    like {!array} and {!map}, or structured using the record builder functions.

    {2 Building Records}

    Records are built using a pipeline of functions:

    {[
      type person = { name: string; age: int }

      let person_codec =
        record (Type_name.simple "Person") (fun name age -> { name; age })
        |> field "name" string (fun p -> p.name)
        |> field "age" int (fun p -> p.age)
        |> finish
    ]}

    The pipeline starts with {!record}, which takes a type name and a constructor
    function. Then {!field} is called for each field, providing the field name,
    the codec for that field type, and a getter function. Finally, {!finish}
    completes the record definition.

    {2 Optional Fields}

    Optional fields use {!field_opt} instead of {!field}:

    {[
      type user = { username: string; email: string option }

      let user_codec =
        record (Type_name.simple "User") (fun username email -> { username; email })
        |> field "username" string (fun u -> u.username)
        |> field_opt "email" string (fun u -> u.email)
        |> finish
    ]}

    {2 Nested Records}

    Codecs can be composed to create nested structures:

    {[
      type address = { street: string; city: string }

      let address_codec =
        record (Type_name.simple "Address") (fun street city -> { street; city })
        |> field "street" string (fun a -> a.street)
        |> field "city" string (fun a -> a.city)
        |> finish

      type person_with_address = { name: string; address: address }

      let person_codec =
        record (Type_name.simple "Person") (fun name address -> { name; address })
        |> field "name" string (fun p -> p.name)
        |> field "address" address_codec (fun p -> p.address)
        |> finish
    ]}

    {2 Arrays and Maps}

    Container types are built using {!array} and {!map}:

    {[
      (* Array of integers *)
      let int_array_codec = array int

      (* Map with string values *)
      let string_map_codec = map string
    ]}

    {2 Unions and Options}

    Union types allow a value to be one of several alternatives. The {!option}
    codec is a common special case for nullable values:

    {[
      (* Optional value (union of null and int) *)
      let opt_int_codec = option int

      (* General union *)
      let int_or_string = union [int; string]
    ]}

    {1 Encoding and Decoding}

    Once you have a codec, use the convenience functions to encode and decode:

    {[
      (* Encode to bytes *)
      let bytes = encode_to_bytes person_codec { name = "Alice"; age = 30 }

      (* Decode from bytes *)
      let person = decode_from_bytes person_codec bytes
    ]}
*)

(** The core codec type.

    A codec ['a t] describes how to serialize and deserialize values of type ['a].
    It contains:
    - [schema]: The Avro schema for this type
    - [encode]: Function to write a value to binary output
    - [decode]: Function to read a value from binary input
*)
type 'a t = {
  schema: Schema.t;
  encode: 'a -> Output.t -> unit;
  decode: Input.t -> 'a;
}

(** {1 Primitive Codecs} *)

(** Codec for Avro null type.

    The null type has a single value, represented by [()] in OCaml.
    Commonly used in unions to represent optional values. *)
val null : unit t

(** Codec for Avro boolean type.

    Maps to OCaml's [bool] type. *)
val boolean : bool t

(** Codec for Avro int type.

    Maps to OCaml's [int] type (32-bit signed integer).
    Encoded using variable-length zig-zag encoding. *)
val int : int t

(** Codec for Avro long type.

    Maps to OCaml's [int64] type (64-bit signed integer).
    Encoded using variable-length zig-zag encoding. *)
val long : int64 t

(** Codec for Avro float type.

    Maps to OCaml's [float] type, stored as 32-bit IEEE 754 floating-point. *)
val float : float t

(** Codec for Avro double type.

    Maps to OCaml's [float] type, stored as 64-bit IEEE 754 floating-point. *)
val double : float t

(** Codec for Avro bytes type.

    Maps to OCaml's [bytes] type, representing arbitrary binary data.
    Encoded with length prefix followed by raw bytes. *)
val bytes : bytes t

(** Codec for Avro string type.

    Maps to OCaml's [string] type, representing UTF-8 encoded text.
    Encoded with length prefix followed by UTF-8 bytes. *)
val string : string t

(** {1 Fixed-Length Types} *)

(** Codec for Avro fixed type.

    Creates a codec for fixed-length byte sequences. The size must match exactly
    when encoding or decoding.

    @param name Optional name for the fixed type (default: "fixed")
    @param size Number of bytes (must be positive)

    Example:
    {[
      let mac_address_codec = fixed ~name:"MacAddress" 6
      let mac = Bytes.of_string "\\x00\\x1a\\x2b\\x3c\\x4d\\x5e"
      let encoded = encode_to_bytes mac_address_codec mac
    ]}
*)
val fixed : ?name:string -> int -> bytes t

(** {1 Container Types} *)

(** Codec for Avro array type.

    Creates a codec for arrays where all elements have the same type.

    @param codec The codec for array elements

    Example:
    {[
      let int_array_codec = array int
      let numbers = [| 1; 2; 3; 4; 5 |]
      let encoded = encode_to_bytes int_array_codec numbers
    ]}

    Arrays can contain any type, including records:
    {[
      let person_array_codec = array person_codec
      let people = [| { name = "Alice"; age = 30 }; { name = "Bob"; age = 25 } |]
    ]}
*)
val array : 'a t -> 'a array t

(** Codec for Avro map type.

    Creates a codec for maps with string keys and homogeneous values.
    Represented as association lists [(string * 'a) list] in OCaml.

    @param codec The codec for map values (keys are always strings)

    Example:
    {[
      let scores_codec = map int
      let scores = [("alice", 100); ("bob", 95); ("charlie", 87)]
      let encoded = encode_to_bytes scores_codec scores
    ]}
*)
val map : 'a t -> (string * 'a) list t

(** {1 Union Types} *)

(** Codec for Avro union type.

    Creates a codec for union types, where a value can be one of several alternatives.
    Values are represented as [(int * 'a)] tuples, where the int is the branch index
    and 'a is the value for that branch.

    Note: This is a low-level function. Consider using {!option} for nullable values.

    @param codecs List of codecs for each branch

    Example:
    {[
      let int_or_string = union [int; string]
      let value1 = (0, 42)        (* int branch *)
      let value2 = (1, "hello")   (* string branch *)
    ]}
*)
val union : 'a t list -> (int * 'a) t

(** Codec for optional values.

    A convenience function that creates a union of null and the given type.
    This is the standard way to represent nullable values in Avro.

    @param codec The codec for the non-null case

    Example:
    {[
      let optional_int = option int
      let some_value = Some 42
      let no_value = None
      let encoded1 = encode_to_bytes optional_int some_value
      let encoded2 = encode_to_bytes optional_int no_value
    ]}
*)
val option : 'a t -> 'a option t

(** {1 Record Building} *)

(** Record builder type.

    This type is used internally to accumulate field definitions while building
    a record codec. Users interact with it through {!record}, {!field}, {!field_opt},
    and {!finish}.

    Type parameters:
    - ['record] is the final record type
    - ['constructor] is the partially applied constructor function
*)
type ('record, 'constructor) builder = {
  type_name: Type_name.t;
  constructor: 'constructor;
  fields_rev: Schema.field list;
  encode: 'record -> Output.t -> unit;
  decode: Input.t -> 'constructor;
}

(** Start building a record codec.

    This begins a pipeline for defining a record type. The record is built by
    chaining {!field} or {!field_opt} calls, and completed with {!finish}.

    @param type_name The name for this record type
    @param constructor A function that constructs the record from its fields

    Example:
    {[
      type person = { name: string; age: int }

      let person_codec =
        record (Type_name.simple "Person") (fun name age -> { name; age })
        |> field "name" string (fun p -> p.name)
        |> field "age" int (fun p -> p.age)
        |> finish
    ]}

    The constructor function should have one parameter for each field, in the
    order they will be added with {!field} calls.
*)
val record : Type_name.t -> 'constructor -> ('record, 'constructor) builder

(** Add a required field to a record.

    Adds a field definition to the record being built. This function is designed
    to be used with the pipe operator [|>] for a fluent interface.

    @param field_name The name of the field in the Avro schema
    @param field_codec The codec for this field's type
    @param getter A function to extract this field's value from the record
    @param builder The record builder to add the field to

    Example:
    {[
      let person_codec =
        record (Type_name.simple "Person") (fun name age -> { name; age })
        |> field "name" string (fun p -> p.name)
        |> field "age" int (fun p -> p.age)
        |> finish
    ]}

    Fields are encoded and decoded in the order they are added.
*)
val field : string -> 'field t -> ('record -> 'field) -> ('record, 'field -> 'rest) builder -> ('record, 'rest) builder

(** Add an optional field to a record.

    Like {!field}, but for optional fields that may be [None]. The field will
    be encoded as a union of null and the field type, with a default value of null.

    @param field_name The name of the field in the Avro schema
    @param field_codec The codec for this field's non-null type
    @param getter A function to extract this field's [option] value from the record
    @param builder The record builder to add the field to

    Example:
    {[
      type user = { username: string; email: string option }

      let user_codec =
        record (Type_name.simple "User") (fun username email -> { username; email })
        |> field "username" string (fun u -> u.username)
        |> field_opt "email" string (fun u -> u.email)
        |> finish
    ]}
*)
val field_opt : string -> 'field t -> ('record -> 'field option) -> ('record, 'field option -> 'rest) builder -> ('record, 'rest) builder

(** Complete the record definition and return the codec.

    This function finalizes the record builder and returns a complete codec
    that can be used for encoding and decoding.

    @param builder The record builder to finalize

    Example:
    {[
      let person_codec =
        record (Type_name.simple "Person") (fun name age -> { name; age })
        |> field "name" string (fun p -> p.name)
        |> field "age" int (fun p -> p.age)
        |> finish  (* Returns a person t codec *)
    ]}
*)
val finish : ('record, 'record) builder -> 'record t

(** {1 Convenience Functions} *)

(** Encode a value to bytes.

    This is a convenience function that creates an output buffer, encodes
    the value, and returns the resulting bytes.

    @param codec The codec to use for encoding
    @param value The value to encode
    @return The encoded bytes

    Example:
    {[
      let person = { name = "Alice"; age = 30 }
      let bytes = encode_to_bytes person_codec person
    ]}
*)
val encode_to_bytes : 'a t -> 'a -> bytes

(** Decode a value from bytes.

    This is a convenience function that creates an input buffer from bytes
    and decodes a value.

    @param codec The codec to use for decoding
    @param bytes The bytes to decode
    @return The decoded value

    Example:
    {[
      let person = decode_from_bytes person_codec bytes
      (* person : { name = "Alice"; age = 30 } *)
    ]}

    Raises [Failure] if the bytes cannot be decoded according to the schema.
*)
val decode_from_bytes : 'a t -> bytes -> 'a

(** Encode a value to a string.

    Like {!encode_to_bytes}, but returns a string instead of bytes.
    Useful for debugging or when working with string-based APIs.

    @param codec The codec to use for encoding
    @param value The value to encode
    @return The encoded data as a string

    Note: The resulting string contains binary data and may not be
    printable or valid UTF-8.
*)
val encode_to_string : 'a t -> 'a -> string

(** Decode a value from a string.

    Like {!decode_from_bytes}, but accepts a string instead of bytes.

    @param codec The codec to use for decoding
    @param str The string containing encoded data
    @return The decoded value

    Raises [Failure] if the string cannot be decoded according to the schema.
*)
val decode_from_string : 'a t -> string -> 'a

(** {1 Recursive Types} *)

(** Create a codec for recursive types using a fixpoint combinator.

    This combinator allows you to define codecs for self-referential types
    (like linked lists or trees) without manually managing mutable references.

    The function you provide receives a "self" codec that can be used to
    refer to the type being defined. This self-reference is safe because
    it uses internal thunking to delay evaluation until encoding/decoding time.

    {2 Linked List Example}

    {[
      type linked_node = { value: int; next: linked_node option }

      let linked_node_codec =
        recursive (fun self ->
          record (Type_name.simple "LinkedNode") (fun value next -> { value; next })
          |> field "value" int (fun r -> r.value)
          |> field "next" (option self) (fun r -> r.next)
          |> finish
        )
    ]}

    {2 Binary Tree Example}

    {[
      type tree_node = { tree_value: int; left: tree_node option; right: tree_node option }

      let tree_node_codec =
        recursive (fun self ->
          record (Type_name.simple "TreeNode")
            (fun tree_value left right -> { tree_value; left; right })
          |> field "value" int (fun r -> r.tree_value)
          |> field "left" (option self) (fun r -> r.left)
          |> field "right" (option self) (fun r -> r.right)
          |> finish
        )
    ]}

    @param f A function that takes a "self" codec and returns the complete codec definition
    @return A codec that properly handles recursive encoding and decoding

    Note: The schema generated will inline the full record definition. For proper
    Avro recursive schemas with name references, additional schema normalization
    may be needed.
*)
val recursive : ('a t -> 'a t) -> 'a t
