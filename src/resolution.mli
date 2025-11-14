(** Schema resolution for schema evolution.

    This module implements Avro's schema resolution algorithm, which enables
    schema evolution by allowing data written with one schema (writer schema)
    to be read with a different but compatible schema (reader schema).

    Schema resolution handles:
    - Type promotions (e.g., int to long, float to double)
    - Field additions and removals with default values
    - Field reordering
    - Union branch matching
    - Enum symbol mapping with defaults
    - Named type aliases

    The resolution process produces a [read_schema] that encodes all the
    transformations needed to decode writer data into the reader's expected format.
*)

(** {1 Resolved Schema Types} *)

(** A field in a resolved record schema.

    Contains information about how to map a field from the writer's data
    to the reader's expected structure. *)
type read_field = {
  field_name: string;
  (** The name of the field *)

  field_schema: read_schema;
  (** The resolved schema for this field's data *)

  field_position: int option;
  (** Position in the reader's record structure.
      - [Some pos]: Field exists in reader at position [pos]
      - [None]: Field exists only in writer and should be skipped during decoding *)
}

(** A resolved schema that combines reader and writer schema information.

    The [read_schema] type encodes all type promotions, field mappings, and
    default values needed to transform writer data into reader format. This is
    the core data structure produced by schema resolution.

    Type promotions are explicitly represented:
    - Numeric promotions: [Int_as_long], [Int_as_float], [Long_as_double], etc.
    - String/Bytes interchangeability

    Complex type resolution:
    - [Record]: Maps writer fields to reader fields, handles reordering and defaults
    - [Enum]: Maps writer symbol indices to reader symbol indices
    - [Union]: Maps each writer branch to the appropriate reader branch
    - [As_union]: Wraps a non-union writer type into a union reader type
*)
and read_schema =
  | Null
  (** Null type - no promotion *)

  | Boolean
  (** Boolean type - no promotion *)

  | Int
  (** Integer type - no promotion *)

  | Int_as_long
  (** Type promotion: writer has int, reader expects long *)

  | Int_as_float
  (** Type promotion: writer has int, reader expects float *)

  | Int_as_double
  (** Type promotion: writer has int, reader expects double *)

  | Long
  (** Long integer type - no promotion *)

  | Long_as_float
  (** Type promotion: writer has long, reader expects float *)

  | Long_as_double
  (** Type promotion: writer has long, reader expects double *)

  | Float
  (** Float type - no promotion *)

  | Float_as_double
  (** Type promotion: writer has float, reader expects double *)

  | Double
  (** Double type - no promotion *)

  | Bytes
  (** Bytes type - may represent bytes or string from writer *)

  | String
  (** String type - may represent string or bytes from writer *)

  | Array of read_schema
  (** Array with resolved element schema *)

  | Map of read_schema
  (** Map with resolved value schema (keys are always strings) *)

  | Record of {
      name: Type_name.t;
      (** The reader's record name *)

      fields: read_field list;
      (** Fields in writer order, including fields to skip *)

      defaults: (int * string * Schema.default) list;
      (** Default values for fields that exist in reader but not writer.
          Each tuple is (reader_position, field_name, default_value). *)
    }
  (** Resolved record schema with field mappings and defaults *)

  | Enum of {
      name: Type_name.t;
      (** The reader's enum name *)

      symbols: string list;
      (** Reader's symbol list for validation and lookups *)

      symbol_map: int array;
      (** Maps writer symbol index to reader symbol index.
          [symbol_map.(writer_idx)] gives the corresponding reader index. *)
    }
  (** Resolved enum schema with symbol index mapping *)

  | Union of (int * read_schema) array
  (** Resolved union-to-union mapping.
      Array indexed by writer branch index, each element is
      [(reader_branch_index, resolved_schema)] for that writer branch. *)

  | As_union of int * read_schema
  (** Wraps a non-union writer type into a union reader type.
      First argument is the reader branch index, second is the resolved schema. *)

  | Fixed of Type_name.t * int
  (** Fixed-length bytes with name and size *)

  | Named_type of Type_name.t
  (** Reference to a named type (for recursive types) *)

(** {1 Resolution Errors} *)

(** Errors that can occur during schema resolution.

    These errors indicate incompatibilities between reader and writer schemas
    that prevent successful schema evolution. *)
type mismatch =
  | Type_mismatch of Schema.t * Schema.t
  (** Incompatible types that cannot be resolved.
      Arguments are (reader_schema, writer_schema). *)

  | Missing_field of Type_name.t * string
  (** Reader requires a field that writer doesn't provide and no default value exists.
      Arguments are (record_name, field_name). *)

  | Field_mismatch of Type_name.t * string
  (** Field exists in both schemas but their types are incompatible.
      Arguments are (record_name, field_name). *)

  | Missing_union_branch of Type_name.t
  (** Writer has a union branch that doesn't match any reader branch.
      Argument is the type name identifier. *)

  | Missing_symbol of string
  (** Enum symbol exists in writer but not in reader and no default is specified.
      Argument is the symbol name. *)

  | Fixed_size_mismatch of Type_name.t * int * int
  (** Fixed types have the same name but different sizes.
      Arguments are (type_name, reader_size, writer_size). *)

  | Named_type_unresolved of Type_name.t
  (** A named type reference couldn't be resolved.
      Argument is the type name. *)

(** {1 Error Formatting} *)

(** Convert a resolution mismatch error to a human-readable string.

    @param mismatch The error to format
    @return A descriptive error message *)
val error_to_string : mismatch -> string

(** {1 Schema Resolution} *)

(** Environment for tracking named type mappings during resolution.

    Used to handle recursive types by mapping writer type names to reader
    type names. Each entry is [(writer_name, reader_name)]. *)
type environment = (Type_name.t * Type_name.t) list

(** Resolve reader and writer schemas to produce a resolved schema.

    This is the main entry point for schema resolution. It analyzes the
    reader and writer schemas and produces a [read_schema] that describes
    how to transform writer data into reader format.

    The resolution algorithm follows the Avro specification:
    1. Matching types with identical structure resolve trivially
    2. Numeric types can be promoted (int->long->float->double)
    3. String and bytes are interchangeable
    4. Records match by name (including aliases), fields match by name
    5. Enums match by name, symbols map with optional default
    6. Unions can match non-union types and other unions
    7. Arrays and maps resolve their element/value types recursively

    @param reader The schema expected by the reader
    @param writer The schema used to write the data
    @return [Ok read_schema] if schemas are compatible, [Error mismatch] otherwise *)
val resolve_schemas : Schema.t -> Schema.t -> (read_schema, mismatch) result

(** Internal recursive deconflict function with explicit environment.

    This function performs the core resolution logic while maintaining an
    environment of named type mappings for handling recursive types.

    @param env Environment tracking named type mappings
    @param reader The reader's schema
    @param writer The writer's schema
    @return [Ok read_schema] if compatible, [Error mismatch] otherwise *)
val deconflict : environment -> Schema.t -> Schema.t -> (read_schema, mismatch) result

(** Find which reader union branch matches a writer type.

    Attempts to resolve the writer type against each branch in the reader's
    union, returning the first successful match.

    @param reader_branches List of schemas in the reader's union
    @param writer_type The writer's schema to match
    @return [Some (branch_index, resolved_schema)] if a match is found, [None] otherwise *)
val find_union_branch : Schema.t list -> Schema.t -> (int * read_schema) option
