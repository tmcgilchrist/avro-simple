(** Qualified type names for named Avro types.

    In Avro, named types (records, enums, and fixed) have qualified names
    consisting of a base name and an optional namespace. This module provides
    utilities for creating, parsing, and manipulating these qualified names.

    {1 Overview}

    A qualified name consists of:
    - A base name (e.g., "Person")
    - An optional namespace (e.g., ["com"; "example"])

    The full name is formed by joining the namespace parts and the base name
    with dots (e.g., "com.example.Person").

    {2 Examples}

    {[
      (* Simple name without namespace *)
      let person = Type_name.simple "Person"
      (* full_name person = "Person" *)

      (* Name with namespace *)
      let person = Type_name.make "Person" ["com"; "example"]
      (* full_name person = "com.example.Person" *)

      (* Parse a full name string *)
      let person = Type_name.parse "com.example.Person"
      (* person.name = "Person", person.namespace = ["com"; "example"] *)
    ]}
*)

(** The type representing a qualified name.

    Contains a base name and a list of namespace components.
*)
type t = {
  name: string;  (** The base name *)
  namespace: string list;  (** The namespace components *)
}

(** Create a qualified name with a namespace.

    @param name The base name
    @param namespace List of namespace components (e.g., ["com"; "example"])
    @return A qualified type name

    Example:
    {[
      let person = make "Person" ["com"; "example"]
      (* Represents "com.example.Person" *)
    ]}
*)
val make : string -> string list -> t

(** Create a simple name without a namespace.

    This is a convenience function for creating unqualified names.

    @param name The base name
    @return A qualified type name with empty namespace

    Example:
    {[
      let person = simple "Person"
      (* Represents just "Person" *)
    ]}
*)
val simple : string -> t

(** Parse a fully qualified name string.

    Splits a dotted name string into namespace and base name components.

    @param full_name A dotted name string (e.g., "com.example.Person")
    @return A qualified type name

    Examples:
    {[
      let t1 = parse "Person"
      (* t1 = { name = "Person"; namespace = [] } *)

      let t2 = parse "com.example.Person"
      (* t2 = { name = "Person"; namespace = ["com"; "example"] } *)
    ]}
*)
val parse : string -> t

(** Get the full qualified name as a string.

    Joins the namespace components and base name with dots.

    @param t The type name
    @return The full name string

    Examples:
    {[
      full_name (simple "Person")  (* = "Person" *)
      full_name (make "Person" ["com"; "example"])  (* = "com.example.Person" *)
    ]}
*)
val full_name : t -> string

(** Get the base name (without namespace).

    @param t The type name
    @return The base name only

    Example:
    {[
      base_name (parse "com.example.Person")  (* = "Person" *)
    ]}
*)
val base_name : t -> string

(** Get the namespace as a string option.

    @param t The type name
    @return [Some namespace] if present, [None] if empty

    Examples:
    {[
      namespace (simple "Person")  (* = None *)
      namespace (make "Person" ["com"; "example"])  (* = Some "com.example" *)
    ]}
*)
val namespace : t -> string option

(** Check if two type names are equal.

    Two names are equal if both their base names and namespaces match.

    @param t1 First type name
    @param t2 Second type name
    @return [true] if equal, [false] otherwise
*)
val equal : t -> t -> bool

(** Compare two type names lexicographically.

    Compares the full qualified names.

    @param t1 First type name
    @param t2 Second type name
    @return Negative if t1 < t2, zero if equal, positive if t1 > t2
*)
val compare : t -> t -> int

(** Check if two type names are compatible.

    Currently checks for exact equality. Alias support may be added later
    for more flexible schema evolution.

    @param t1 First type name
    @param t2 Second type name
    @return [true] if compatible, [false] otherwise
*)
val compatible : t -> t -> bool

(** Check if reader and writer names are compatible considering aliases.

    This function is used during schema resolution to determine if a
    writer schema can be read with a reader schema. Names are compatible if:
    - Base names match (ignoring namespace), OR
    - Writer's full name appears in reader's alias list

    This follows the Avro specification for schema resolution with aliases.

    @param reader_name The reader's type name
    @param reader_aliases List of alias names for the reader
    @param writer_name The writer's type name
    @return [true] if compatible, [false] otherwise
*)
val compatible_names : reader_name:t -> reader_aliases:string list -> writer_name:t -> bool
