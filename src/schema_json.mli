(** JSON schema parsing and serialization *)

(** Error types that can occur during schema parsing *)
type parse_error =
  | InvalidType of string        (** Invalid type specification *)
  | MissingField of string       (** Required field is missing *)
  | InvalidUnion of string       (** Invalid union definition *)
  | InvalidNamespace of string   (** Invalid namespace specification *)
  | UnknownType of string        (** Reference to unknown type *)
  | ParseError of string         (** General parse error *)

(** Exception raised when schema parsing fails *)
exception Schema_parse_error of parse_error

(** Convert a parse error to a human-readable string *)
val error_to_string : parse_error -> string

(** Context for parsing - tracks named types and current namespace *)
type parse_context = {
  mutable namespace: string option;
  named_types: (string, Schema.t) Hashtbl.t;
}

(** Create a new parsing context *)
val create_context : unit -> parse_context

(** Resolve a type name with namespace *)
val resolve_name : parse_context -> string -> string

(** Parse a primitive type from a string *)
val parse_primitive : string -> Schema.t

(** Parse default value from JSON *)
val parse_default : parse_context -> Schema.t -> Yojson.Basic.t -> Schema.default

(** Parse a field from JSON *)
val parse_field : parse_context -> Yojson.Basic.t -> Schema.field

(** Parse a record schema *)
val parse_record : parse_context -> Yojson.Basic.t -> Schema.t

(** Parse an enum schema *)
val parse_enum : parse_context -> Yojson.Basic.t -> Schema.t

(** Parse a fixed schema *)
val parse_fixed : parse_context -> Yojson.Basic.t -> Schema.t

(** Parse an array schema *)
val parse_array : parse_context -> Yojson.Basic.t -> Schema.t

(** Parse a map schema *)
val parse_map : parse_context -> Yojson.Basic.t -> Schema.t

(** Parse a union schema *)
val parse_union : parse_context -> Yojson.Basic.t list -> Schema.t

(** Parse a schema from JSON *)
val parse_schema : parse_context -> Yojson.Basic.t -> Schema.t

(** Parse schema from JSON string *)
val of_string : string -> (Schema.t, string) result

(** Parse schema from Yojson.Basic.t value *)
val of_json : Yojson.Basic.t -> (Schema.t, string) result

(** Convert schema to JSON with all metadata (including logical types) *)
val schema_to_json_full : Schema.t -> Yojson.Basic.t

(** Convert schema to JSON string (using canonical form from Fingerprint module) *)
val to_string : Schema.t -> string

(** Convert schema to Yojson.Basic.t (with all metadata including logical types) *)
val to_json : Schema.t -> Yojson.Basic.t

(** Convert schema to JSON string (with all metadata) *)
val to_string_full : Schema.t -> string
