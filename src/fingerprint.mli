(** Schema fingerprinting using CRC-64-AVRO algorithm.

    This module provides functions to compute fingerprints of Avro schemas using
    the CRC-64-AVRO algorithm as specified in the Avro specification. Schema
    fingerprints are used for schema resolution, caching, and validation.

    The fingerprinting process involves two steps:
    1. Convert the schema to its Parsing Canonical Form (PCF) - a normalized
       JSON representation
    2. Compute a CRC-64-AVRO checksum of the canonical form

    The Parsing Canonical Form is a JSON representation with:
    - Whitespace removed
    - Field ordering normalized
    - Logical types and other annotations stripped
    - Names fully qualified

    The CRC-64-AVRO algorithm uses the polynomial 0xC96C5795D7870F42 and
    produces a 64-bit fingerprint that uniquely identifies a schema structure.
*)

val to_canonical_json : Schema.t -> string
(** [to_canonical_json schema] converts an Avro schema to its Parsing Canonical
    Form (PCF) representation as a JSON string.

    The canonical form is a normalized representation of the schema where:
    - All whitespace is removed
    - Logical type annotations are stripped (e.g., [int] with logical type becomes ["int"])
    - Names are fully qualified
    - Fields appear in a specific order
    - Union branches maintain their order

    Examples:
    - Primitive types become simple strings: ["null"], ["boolean"], ["int"]
    - Records include name, type, and fields in that order
    - Arrays include type and items
    - Maps include type and values
    - Enums include name, type, and symbols

    @param schema The schema to convert
    @return A JSON string in Parsing Canonical Form
*)

val crc64_poly : int64
(** The CRC-64-AVRO polynomial value: 0xC96C5795D7870F42.

    This polynomial is used in the CRC-64-AVRO algorithm for computing
    schema fingerprints. It was chosen by the Avro specification to provide
    good error detection properties for schema data.
*)

val crc64_table : int64 array
(** Precomputed lookup table for CRC-64-AVRO calculations.

    This 256-entry table is computed once at module initialization time
    for efficiency. Each entry represents the CRC-64 value for a single
    byte value (0-255) using the CRC-64-AVRO polynomial.

    The table-driven approach significantly speeds up fingerprint computation
    by avoiding bit-by-bit calculation for each byte.
*)

val crc64_of_string : string -> int64
(** [crc64_of_string str] computes the CRC-64-AVRO checksum of a string.

    This function implements the CRC-64-AVRO algorithm using a table-driven
    approach for efficiency. It processes the input string byte-by-byte,
    updating a 64-bit CRC value using the precomputed lookup table.

    The algorithm:
    1. Initializes CRC to all 1s (0xFFFFFFFFFFFFFFFF)
    2. For each byte: XOR with current CRC, look up in table, shift and XOR
    3. Returns the final CRC value (not inverted, unlike some CRC variants)

    @param str The string to compute the checksum for (typically canonical JSON)
    @return The 64-bit CRC-64-AVRO checksum
*)

val crc64 : Schema.t -> int64
(** [crc64 schema] computes the CRC-64-AVRO fingerprint of an Avro schema.

    This is the primary function for generating schema fingerprints. It:
    1. Converts the schema to Parsing Canonical Form
    2. Computes the CRC-64-AVRO checksum of the canonical JSON string
    3. Returns the resulting 64-bit fingerprint

    The fingerprint uniquely identifies the schema structure and can be used for:
    - Schema resolution and compatibility checking
    - Caching compiled schemas
    - Validating that data matches a specific schema version

    Two schemas with the same structure will always produce the same fingerprint,
    even if they differ in documentation, field order (for some types), or other
    non-structural attributes.

    @param schema The Avro schema to fingerprint
    @return A 64-bit fingerprint uniquely identifying the schema structure
*)

val rabin_fingerprint : Schema.t -> int64
(** [rabin_fingerprint schema] computes a Rabin fingerprint of an Avro schema.

    Note: This is currently implemented as an alias to {!crc64} for simplicity.
    A full Rabin fingerprinting implementation may be provided in the future.

    Rabin fingerprinting is an alternative fingerprinting algorithm that could
    be used for schema identification, though CRC-64-AVRO is the standard
    algorithm specified by Apache Avro.

    @param schema The Avro schema to fingerprint
    @return A 64-bit fingerprint (currently computed using CRC-64-AVRO)
*)
