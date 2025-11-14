(** Avro schema representation.

    This module defines the types representing Avro schemas. Avro schemas
    describe the structure of data and are used for both serialization
    and schema evolution.

    {1 Schema Types}

    Avro supports primitive types, complex types (records, arrays, maps, unions),
    and named types (enums, fixed). Schemas can also include logical type
    annotations that add semantic meaning to primitive types.

    {2 Primitive Types}

    The primitive types are: [Null], [Boolean], [Int], [Long], [Float], [Double],
    [Bytes], and [String]. Some primitives can have logical type annotations:
    - [Int] can be "date" or "time-millis"
    - [Long] can be "timestamp-millis", "timestamp-micros", "time-micros"
    - [Bytes] can be "decimal"
    - [String] can be "uuid"

    {2 Complex Types}

    Complex types include:
    - [Array] - homogeneous arrays
    - [Map] - string-keyed maps
    - [Record] - structured records with named fields
    - [Union] - values that can be one of several types
    - [Enum] - enumeration of named symbols
    - [Fixed] - fixed-length byte sequences
*)

(** The main schema type.

    Represents all possible Avro schema types. Primitive types like [Null],
    [Boolean], [Float], and [Double] are simple variants. Other types like
    [Int], [Long], [Bytes], and [String] can optionally carry a logical type
    annotation. Complex types like [Record], [Enum], [Array], [Map], [Union],
    and [Fixed] contain additional schema information.
*)
type t =
  | Null
  | Boolean
  | Int of string option  (** Optional logical type (e.g., "date", "time-millis") *)
  | Long of string option  (** Optional logical type (e.g., "timestamp-micros") *)
  | Float
  | Double
  | Bytes of string option  (** Optional logical type (e.g., "decimal") *)
  | String of string option  (** Optional logical type (e.g., "uuid") *)
  | Array of t  (** Array with element type *)
  | Map of t  (** Map with value type (keys are always strings) *)
  | Record of record_schema  (** Record with named fields *)
  | Enum of enum_schema  (** Enumeration of symbols *)
  | Union of t list  (** Union of multiple types *)
  | Fixed of fixed_schema  (** Fixed-length bytes *)

(** Record schema definition.

    Describes a record type with a name, list of fields, optional documentation,
    and optional aliases for schema evolution.
*)
and record_schema = {
  name: Type_name.t;  (** Qualified name of the record *)
  fields: field list;  (** List of fields (order matters) *)
  record_doc: string option;  (** Optional documentation *)
  record_aliases: string list;  (** Alternative names for schema evolution *)
}

(** Record field definition.

    Describes a single field within a record, including its name, type,
    optional documentation, default value, and aliases.
*)
and field = {
  field_name: string;  (** Name of the field *)
  field_type: t;  (** Schema of the field *)
  field_doc: string option;  (** Optional documentation *)
  field_default: default option;  (** Default value for schema evolution *)
  field_aliases: string list;  (** Alternative names for schema evolution *)
}

(** Enumeration schema definition.

    Describes an enum type with a name, list of valid symbols, optional
    documentation, optional default symbol, and aliases.
*)
and enum_schema = {
  enum_name: Type_name.t;  (** Qualified name of the enum *)
  symbols: string list;  (** List of valid symbol names *)
  enum_doc: string option;  (** Optional documentation *)
  enum_default: string option;
  (** Default symbol to use when writer has symbol not in reader *)
  enum_aliases: string list;  (** Alternative names for schema evolution *)
}

(** Fixed-length bytes schema definition.

    Describes a fixed-length byte sequence with a name, size, optional
    documentation, aliases, and optional logical type annotation.
*)
and fixed_schema = {
  fixed_name: Type_name.t;  (** Qualified name of the fixed type *)
  size: int;  (** Number of bytes (must be positive) *)
  fixed_doc: string option;  (** Optional documentation *)
  fixed_aliases: string list;  (** Alternative names for schema evolution *)
  fixed_logical: string option;  (** Optional logical type (e.g., "decimal", "duration") *)
}

(** Default value representation.

    Default values are used in schema evolution to provide values for fields
    that exist in the reader schema but not in the writer schema.
*)
and default =
  | Null_default
  | Bool_default of bool
  | Int_default of int
  | Long_default of int64
  | Float_default of float
  | Double_default of float
  | Bytes_default of bytes
  | String_default of string
  | Enum_default of string  (** Symbol name *)
  | Array_default of default list
  | Map_default of (string * default) list
  | Union_default of int * default  (** Branch index and value *)

(** {1 Schema Validation} *)

(** Check for duplicate field names in a record.

    Returns [None] if all field names are unique, or [Some name] if
    a duplicate field name is found.

    @param fields List of fields to check
    @return [None] if valid, [Some field_name] if duplicate found
*)
val has_duplicate_fields : field list -> string option

(** Check for duplicate symbols in an enum.

    Returns [None] if all symbols are unique, or [Some symbol] if
    a duplicate is found.

    @param symbols List of symbol names to check
    @return [None] if valid, [Some symbol] if duplicate found
*)
val has_duplicate_symbols : string list -> string option

(** Check if a name is valid according to Avro naming rules.

    Valid names:
    - Must start with [A-Za-z_]
    - Subsequent characters can be [A-Za-z0-9_]
    - Must not be empty

    @param name The name to validate
    @return [true] if valid, [false] otherwise
*)
val is_valid_name : string -> bool

(** Validate a schema recursively.

    Checks for:
    - Unions must have at least 2 branches
    - Unions cannot contain other unions
    - Unions cannot have duplicate types
    - Records must have at least one field
    - Records cannot have duplicate field names
    - Field names must be valid
    - Enums must have at least one symbol
    - Enums cannot have duplicate symbols
    - Symbol names must be valid
    - Fixed size must be positive

    @param schema The schema to validate
    @return [Ok ()] if valid, [Error msg] otherwise
*)
val validate : t -> (unit, string) result

(** Check for name redefinition across the entire schema tree.

    We track schemas we've already seen using physical equality to avoid
    treating the same schema used multiple times as a redefinition.

    @param schema The schema to check
    @return [Ok ()] if no redefinitions, [Error msg] otherwise
*)
val validate_no_name_redefinition : t -> (unit, string) result

(** Full schema validation with all checks.

    Combines [validate] and [validate_no_name_redefinition] to perform
    complete validation of a schema.

    @param schema The schema to validate
    @return [Ok ()] if valid, [Error msg] otherwise
*)
val validate_schema : t -> (unit, string) result

(** Add a logical type to a schema.

    If the schema does not support logical types (e.g., it's a named type
    that isn't [Fixed]), this function has no effect.

    @param logical_type The logical type to add
    @param schema The schema to add the logical type to
    @return The schema with the logical type added
*)
val with_logical_type : string -> t -> t

(** JSON serialization (stub for now).

    @param schema The schema to serialize
    @return JSON string representation
*)
val to_json : t -> string

(** JSON deserialization (stub for now).

    @param json JSON string to deserialize
    @return [Ok schema] if successful, [Error msg] otherwise
*)
val of_json : string -> (t, string) result
