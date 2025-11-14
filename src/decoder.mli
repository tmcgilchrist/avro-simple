(** Avro decoder with schema evolution support.

    This module provides decoding functionality for Avro binary data that supports
    schema evolution. It can read data written with one schema (writer schema) and
    decode it according to a different but compatible schema (reader schema).

    {1 Schema Evolution}

    Avro's schema evolution allows data written with one version of a schema to be
    read with a different version, following these compatibility rules:

    {2 Type Promotions}

    The decoder automatically handles numeric type promotions:
    - int -> long, float, or double
    - long -> float or double
    - float -> double

    {2 Record Evolution}

    Records support field-level evolution:
    - Fields present in the writer but not the reader are skipped
    - Fields present in the reader but not the writer use default values
    - Fields are matched by name, and field order can differ

    {2 Union Evolution}

    - If both schemas are unions, branches are matched by type
    - If the writer schema is not a union but the reader schema is,
      the writer type is matched to a compatible branch in the reader union
    - If the writer schema is a union but the reader schema is not,
      each union branch must be compatible with the reader type

    {2 Enum Evolution}

    Enums support symbol evolution:
    - Symbols are matched by name
    - Symbols in the writer but not the reader are mapped to a default
    - New symbols in the reader must have defaults if they appear in data

    {1 Usage}

    The typical workflow is:
    1. Parse both reader and writer schemas using {!Schema.parse}
    2. Call {!decode_with_schemas} with both schemas and the binary data
    3. The function returns either a decoded {!Value.t} or an error describing
       schema incompatibility

    Example:
    {[
      let reader_schema = Schema.parse reader_json in
      let writer_schema = Schema.parse writer_json in
      match decode_with_schemas reader_schema writer_schema binary_data with
      | Ok value -> (* process decoded value *)
      | Error mismatch -> (* handle schema incompatibility *)
    ]}
*)

(** {1 Core Decoding Functions} *)

val decode_value : Resolution.read_schema -> Input.t -> Value.t
(** [decode_value read_schema inp] decodes a value from input [inp] using the
    resolved read schema.

    The [read_schema] must be obtained by resolving a reader schema against a
    writer schema using {!Resolution.resolve_schemas}. This resolved schema
    contains all the information needed to:
    - Read data in the writer's format
    - Apply type promotions where needed
    - Handle field reordering and defaults for records
    - Map enum symbols between schemas
    - Route union branches correctly

    @param read_schema A resolved read schema from {!Resolution.resolve_schemas}
    @param inp The input stream containing binary Avro data
    @return The decoded value

    @raise Failure if a [Named_type] appears (these should be resolved before decoding)
    @raise various Input exceptions if the binary data is malformed
*)

val decode_with_schemas : Schema.t -> Schema.t -> bytes -> (Value.t, Resolution.mismatch) result
(** [decode_with_schemas reader_schema writer_schema bytes] decodes binary Avro
    data that was written with [writer_schema], interpreting it according to
    [reader_schema].

    This is the main entry point for decoding with schema evolution. It:
    1. Resolves the two schemas to determine how to transform data
    2. Creates an input stream from the bytes
    3. Decodes the data applying all necessary transformations

    @param reader_schema The schema to decode data into (how you want to interpret it)
    @param writer_schema The schema that was used to encode the data originally
    @param bytes The binary Avro-encoded data
    @return [Ok value] if schemas are compatible and decoding succeeds,
            [Error mismatch] if schemas are incompatible

    Schema compatibility is determined by {!Resolution.resolve_schemas}.
    Common incompatibilities include:
    - Mismatched primitive types (without valid promotion)
    - Record fields with no default value missing from writer schema
    - Enum symbols in data not present in reader schema
    - Union branches that cannot be matched between schemas
*)
