(** OCaml Avro - Apache Avro serialization library

    This library provides tools for encoding and decoding data using the
    Apache Avro binary format. Avro is a data serialization system that
    provides:

    - Rich data structures
    - Compact, fast binary data format
    - Container file format for storing persistent data
    - Remote procedure call (RPC) support
    - Schema evolution with forward and backward compatibility

    {1 Getting Started}

    The primary way to use this library is by defining {!Codec.t} values that
    describe how to serialize your domain types to and from Avro's binary format.
    Each codec combines three things:

    - An Avro schema
    - An encoder function (OCaml value → binary)
    - A decoder function (binary → OCaml value)

    {2 Basic Example}

    Here's a complete example showing how to define a record type and its codec:

    {[
      open Avro

      (* Define your domain type *)
      type person = {
        name: string;
        age: int;
      }

      (* Create a codec for the person type *)
      let person_codec =
        Codec.record (Type_name.simple "Person") (fun name age -> { name; age })
        |> Codec.field "name" Codec.string (fun p -> p.name)
        |> Codec.field "age" Codec.int (fun p -> p.age)
        |> Codec.finish

      (* Encode a person to bytes *)
      let person = { name = "Alice"; age = 30 }
      let encoded = Codec.encode_to_bytes person_codec person

      (* Decode bytes back to a person *)
      let decoded = Codec.decode_from_bytes person_codec encoded
    ]}

    {2 Working with Container Files}

    For persistent storage, you can write Avro data to container files
    which include the schema and support compression:

    {[
      (* Write data to a container file *)
      let writer = Container_writer.create ~path:"people.avro" ~codec:person_codec () in
      Container_writer.write writer { name = "Alice"; age = 30 };
      Container_writer.write writer { name = "Bob"; age = 25 };
      Container_writer.close writer

      (* Read data from a container file *)
      let reader = Container_reader.open_file ~path:"people.avro" ~codec:person_codec () in
      let people = Container_reader.fold (fun acc p -> p :: acc) [] reader in
      Container_reader.close reader
    ]}

    {2 Using Logical Types}

    The library includes support for Avro's logical types, which provide
    higher-level semantic types built on primitive Avro types:

    {[
      open Avro.Logical

      (* Date logical type (days since epoch) *)
      let date_codec = date
      let today = (2024, 11, 14)
      let encoded_date = Codec.encode_to_bytes date_codec today

      (* UUID logical type (backed by string) *)
      let uuid_codec = uuid
      let id = Uuidm.v4_gen (Random.State.make_self_init ()) ()
      let encoded_uuid = Codec.encode_to_bytes uuid_codec id

      (* Decimal logical type (arbitrary precision) *)
      let decimal_codec = decimal ~precision:10 ~scale:2
      let amount = Z.of_int 9999  (* represents 99.99 with scale=2 *)
      let encoded_decimal = Codec.encode_to_bytes decimal_codec amount
    ]}

    {2 Schema Evolution}

    One of Avro's key features is support for schema evolution. The {!Resolution}
    module provides tools for reading data written with one schema using a
    different (but compatible) schema:

    {[
      (* Original schema *)
      let person_v1_codec =
        Codec.record (Type_name.simple "Person") (fun name -> { name; age = 0 })
        |> Codec.field "name" Codec.string (fun p -> p.name)
        |> Codec.finish

      (* Evolved schema with new field *)
      let person_v2_codec =
        Codec.record (Type_name.simple "Person") (fun name age -> { name; age })
        |> Codec.field "name" Codec.string (fun p -> p.name)
        |> Codec.field "age" Codec.int (fun p -> p.age)
        |> Codec.finish

      (* Resolution handles schema differences *)
      let resolution = Resolution.make
        ~reader_schema:person_v2_codec.schema
        ~writer_schema:person_v1_codec.schema
    ]}

    {1 Core Modules}
*)

(** Schema types and utilities.

    Defines the structure of Avro schemas including primitive types (int, long, string, etc.),
    complex types (records, arrays, maps, unions), and named types (fixed, enum). *)
module Schema = Schema

(** Type name handling for named Avro types.

    Provides utilities for creating and manipulating qualified type names,
    which consist of a name and optional namespace. *)
module Type_name = Type_name

(** Codec combinators for building encoders and decoders.

    This is the primary module for defining how your OCaml types map to Avro schemas.
    It provides:
    - Primitive codecs: {!Codec.int}, {!Codec.string}, {!Codec.boolean}, etc.
    - Container codecs: {!Codec.array}, {!Codec.map}, {!Codec.option}
    - Record builder: {!Codec.record}, {!Codec.field}, {!Codec.finish}
    - Convenience functions: {!Codec.encode_to_bytes}, {!Codec.decode_from_bytes}

    See the {!Codec} module for detailed documentation and examples. *)
module Codec = Codec

(** Low-level binary output operations.

    Provides functions for writing Avro's binary encoding format. Most users
    will use {!Codec} instead of this module directly. *)
module Output = Output

(** Low-level binary input operations.

    Provides functions for reading Avro's binary encoding format. Most users
    will use {!Codec} instead of this module directly. *)
module Input = Input

(** Generic Avro value representation.

    Provides a dynamic representation of Avro data that can hold any value
    conforming to an Avro schema. Useful for schema-agnostic data processing. *)
module Value = Value

(** Generic decoder for Avro data.

    Provides functionality for decoding Avro binary data to the generic
    {!Value} representation when you don't have a typed codec. *)
module Decoder = Decoder

(** Schema resolution for schema evolution.

    Handles reading data written with one schema (writer schema) using a
    different but compatible schema (reader schema). This enables schema
    evolution patterns like adding fields with defaults or removing fields. *)
module Resolution = Resolution

(** Schema fingerprinting utilities.

    Provides functions for computing fingerprints of schemas, which can be
    used for schema identification and caching. *)
module Fingerprint = Fingerprint

(** Writing Avro container files.

    Container files store a collection of data objects along with their schema.
    They support:
    - Embedded schema storage
    - Compression (null, deflate)
    - Efficient streaming writes
    - Sync markers for splitting files *)
module Container_writer = Container_writer

(** Reading Avro container files.

    Provides functions for reading Avro container files, with support for:
    - Schema extraction
    - Decompression
    - Streaming reads
    - Random access via sync markers *)
module Container_reader = Container_reader

(** Compression codec registry.

    Manages registration and lookup of compression codecs for container files.
    Built-in codecs (null, deflate) are automatically registered. *)
module Codec_registry = Codec_registry

(** JSON schema parsing and generation.

    Provides functions for converting between Avro schemas and their JSON
    representation, as defined in the Avro specification. *)
module Schema_json = Schema_json

(** Logical types support.

    Provides codecs for Avro logical types, which add semantic meaning to
    primitive types:
    - Temporal: {!Logical.date}, {!Logical.time_millis}, {!Logical.timestamp_millis}
    - Numeric: {!Logical.decimal}
    - String: {!Logical.uuid}
    - Other: {!Logical.val-duration}

    Also includes {!Logical.make_logical} for defining custom logical types. *)
module Logical = Logical

(** Initialize compression codecs.

    This function registers the built-in compression codecs (null and deflate).
    It is called automatically when the module loads, so you typically don't
    need to call it manually.

    Returns [unit]. *)
val init_codecs : unit -> unit
