open Yojson.Basic.Util

type parse_error =
  | InvalidType of string
  | MissingField of string
  | InvalidUnion of string
  | InvalidNamespace of string
  | UnknownType of string
  | ParseError of string

exception Schema_parse_error of parse_error

let error_to_string = function
  | InvalidType s -> Printf.sprintf "Invalid type: %s" s
  | MissingField f -> Printf.sprintf "Missing required field: %s" f
  | InvalidUnion s -> Printf.sprintf "Invalid union: %s" s
  | InvalidNamespace s -> Printf.sprintf "Invalid namespace: %s" s
  | UnknownType t -> Printf.sprintf "Unknown type: %s" t
  | ParseError s -> Printf.sprintf "Parse error: %s" s

type parse_context = {
  mutable namespace: string option;
  named_types: (string, Schema.t) Hashtbl.t;
}

let create_context () = {
  namespace = None;
  named_types = Hashtbl.create 16;
}

let resolve_name ctx name =
  if String.contains name '.' then
    name
  else
    match ctx.namespace with
    | None -> name
    | Some ns -> ns ^ "." ^ name

let parse_primitive = function
  | "null" -> Schema.Null
  | "boolean" -> Schema.Boolean
  | "int" -> Schema.Int None
  | "long" -> Schema.Long None
  | "float" -> Schema.Float
  | "double" -> Schema.Double
  | "bytes" -> Schema.Bytes None
  | "string" -> Schema.String None
  | s -> raise (Schema_parse_error (InvalidType s))

let rec parse_default ctx schema (json : Yojson.Basic.t) : Schema.default =
  match schema, json with
  | Schema.Null, `Null -> Schema.Null_default
  | Schema.Boolean, `Bool b -> Schema.Bool_default b
  | Schema.Int _, `Int i -> Schema.Int_default i
  | Schema.Long _, `Int i -> Schema.Long_default (Int64.of_int i)
  | Schema.Float, `Int i -> Schema.Float_default (float_of_int i)
  | Schema.Float, `Float f -> Schema.Float_default f
  | Schema.Double, `Int i -> Schema.Double_default (float_of_int i)
  | Schema.Double, `Float f -> Schema.Double_default f
  | Schema.String _, `String s -> Schema.String_default s
  | Schema.Bytes _, `String s -> Schema.Bytes_default (Bytes.of_string s)
  | Schema.Array _, `List [] -> Schema.Array_default []
  | Schema.Enum _, `String s -> Schema.Enum_default s
  | Schema.Union branches, _ ->
      (match branches with
       | first :: _ ->
           let default_val = parse_default ctx first json in
           Schema.Union_default (0, default_val)
       | [] -> raise (Schema_parse_error (InvalidUnion "Empty union for default")))
  | _ -> raise (Schema_parse_error (ParseError "Invalid default value for type"))

let parse_schema_impl = ref (fun _ctx _json -> Schema.Null)

let parse_field ctx json =
  let field_name = json |> member "name" |> to_string in
  let field_type_json = json |> member "type" in
  let field_type = !parse_schema_impl ctx field_type_json in

  let field_doc =
    try Some (json |> member "doc" |> to_string)
    with _ -> None
  in

  let field_default =
    try
      let default_json = json |> member "default" in
      Some (parse_default ctx field_type default_json)
    with _ -> None
  in

  let field_aliases =
    try json |> member "aliases" |> to_list |> List.map to_string
    with _ -> []
  in

  { Schema.field_name; field_type; field_doc; field_default; field_aliases }

let parse_record ctx json =
  let record_namespace =
    try Some (json |> member "namespace" |> to_string)
    with _ -> ctx.namespace
  in

  let old_ns = ctx.namespace in
  ctx.namespace <- record_namespace;

  let name_str = json |> member "name" |> to_string in
  let full_name = resolve_name ctx name_str in
  let name = Type_name.parse full_name in

  let doc =
    try Some (json |> member "doc" |> to_string)
    with _ -> None
  in

  let aliases =
    try json |> member "aliases" |> to_list |> List.map to_string
    with _ -> []
  in

  let fields_json = json |> member "fields" |> to_list in
  let fields = List.map (parse_field ctx) fields_json in

  ctx.namespace <- old_ns;

  Schema.Record { name; fields; record_doc = doc; record_aliases = aliases }

let parse_enum ctx json =
  let enum_namespace =
    try Some (json |> member "namespace" |> to_string)
    with _ -> ctx.namespace
  in

  let name_str = json |> member "name" |> to_string in
  let full_name = match enum_namespace with
    | None -> name_str
    | Some ns -> if String.contains name_str '.' then name_str else ns ^ "." ^ name_str
  in
  let enum_name = Type_name.parse full_name in

  let symbols = json |> member "symbols" |> to_list |> List.map to_string in

  let enum_doc =
    try Some (json |> member "doc" |> to_string)
    with _ -> None
  in

  let enum_default =
    try Some (json |> member "default" |> to_string)
    with _ -> None
  in

  let enum_aliases =
    try json |> member "aliases" |> to_list |> List.map to_string
    with _ -> []
  in

  Schema.Enum { enum_name; symbols; enum_doc; enum_default; enum_aliases }

let parse_fixed ctx json =
  let fixed_namespace =
    try Some (json |> member "namespace" |> to_string)
    with _ -> ctx.namespace
  in

  let name_str = json |> member "name" |> to_string in
  let full_name = match fixed_namespace with
    | None -> name_str
    | Some ns -> if String.contains name_str '.' then name_str else ns ^ "." ^ name_str
  in
  let fixed_name = Type_name.parse full_name in

  let size = json |> member "size" |> to_int in

  let fixed_doc =
    try Some (json |> member "doc" |> to_string)
    with _ -> None
  in

  let fixed_aliases =
    try json |> member "aliases" |> to_list |> List.map to_string
    with _ -> []
  in

  let fixed_logical =
    try Some (json |> member "logicalType" |> to_string)
    with _ -> None
  in

  Schema.Fixed { fixed_name; size; fixed_doc; fixed_aliases; fixed_logical }

let parse_array ctx json =
  let items_json = json |> member "items" in
  let items = !parse_schema_impl ctx items_json in
  Schema.Array items

let parse_map ctx json =
  let values_json = json |> member "values" in
  let values = !parse_schema_impl ctx values_json in
  Schema.Map values

let parse_union ctx json_list =
  let branches = List.map (!parse_schema_impl ctx) json_list in
  Schema.Union branches

let parse_schema ctx json =
  match json with
  | `String s ->
      begin try
        parse_primitive s
      with Schema_parse_error (InvalidType _) ->
        try
          Hashtbl.find ctx.named_types (resolve_name ctx s)
        with Not_found ->
          raise (Schema_parse_error (UnknownType s))
      end

  | `Assoc _ ->
      let type_str = json |> member "type" |> to_string in
      begin match type_str with
      | "record" -> parse_record ctx json
      | "enum" -> parse_enum ctx json
      | "fixed" -> parse_fixed ctx json
      | "array" -> parse_array ctx json
      | "map" -> parse_map ctx json
      | "int" | "long" | "bytes" | "string" ->
          let logical_type =
            try Some (json |> member "logicalType" |> to_string)
            with _ -> None
          in
          begin match type_str with
          | "int" -> Schema.Int logical_type
          | "long" -> Schema.Long logical_type
          | "bytes" -> Schema.Bytes logical_type
          | "string" -> Schema.String logical_type
          | _ -> failwith "unreachable"
          end
      | _ -> parse_primitive type_str
      end

  | `List json_list ->
      parse_union ctx json_list

  | _ -> raise (Schema_parse_error (ParseError "Invalid schema format"))

(* TODO What's this construct for? *)
let () = parse_schema_impl := parse_schema

let of_string s =
  try
    let json = Yojson.Basic.from_string s in
    let ctx = create_context () in
    Ok (parse_schema ctx json)
  with
  | Schema_parse_error e -> Error (error_to_string e)
  | Yojson.Json_error msg -> Error (Printf.sprintf "JSON parse error: %s" msg)
  | e -> Error (Printf.sprintf "Unexpected error: %s" (Printexc.to_string e))

let of_json json =
  try
    let ctx = create_context () in
    Ok (parse_schema ctx json)
  with
  | Schema_parse_error e -> Error (error_to_string e)
  | e -> Error (Printf.sprintf "Unexpected error: %s" (Printexc.to_string e))

let rec schema_to_json_full schema =
  match schema with
  | Schema.Null -> `String "null"
  | Schema.Boolean -> `String "boolean"
  | Schema.Int None -> `String "int"
  | Schema.Int (Some lt) -> `Assoc [("type", `String "int"); ("logicalType", `String lt)]
  | Schema.Long None -> `String "long"
  | Schema.Long (Some lt) -> `Assoc [("type", `String "long"); ("logicalType", `String lt)]
  | Schema.Float -> `String "float"
  | Schema.Double -> `String "double"
  | Schema.Bytes None -> `String "bytes"
  | Schema.Bytes (Some lt) -> `Assoc [("type", `String "bytes"); ("logicalType", `String lt)]
  | Schema.String None -> `String "string"
  | Schema.String (Some lt) -> `Assoc [("type", `String "string"); ("logicalType", `String lt)]
  | Schema.Array item -> `Assoc [("type", `String "array"); ("items", schema_to_json_full item)]
  | Schema.Map value -> `Assoc [("type", `String "map"); ("values", schema_to_json_full value)]
  | Schema.Union branches -> `List (List.map schema_to_json_full branches)
  | Schema.Record { name; fields; record_doc; record_aliases } ->
      let base = [
        ("type", `String "record");
        ("name", `String (Type_name.base_name name));
      ] in
      let with_ns = match Type_name.namespace name with
        | None -> base
        | Some ns -> base @ [("namespace", `String ns)]
      in
      let with_doc = match record_doc with
        | None -> with_ns
        | Some doc -> with_ns @ [("doc", `String doc)]
      in
      let with_aliases = match record_aliases with
        | [] -> with_doc
        | aliases -> with_doc @ [("aliases", `List (List.map (fun a -> `String a) aliases))]
      in
      let fields_json = `List (List.map (fun (f : Schema.field) ->
        let field_base = [
          ("name", `String f.field_name);
          ("type", schema_to_json_full f.field_type);
        ] in
        let with_doc = match f.field_doc with
          | None -> field_base
          | Some doc -> field_base @ [("doc", `String doc)]
        in
        let with_default = match f.field_default with
          | None -> with_doc
          | Some _ -> with_doc
        in
        let with_aliases = match f.field_aliases with
          | [] -> with_default
          | aliases -> with_default @ [("aliases", `List (List.map (fun a -> `String a) aliases))]
        in
        `Assoc with_aliases
      ) fields) in
      `Assoc (with_aliases @ [("fields", fields_json)])
  | Schema.Enum { enum_name; symbols; enum_doc; enum_default; enum_aliases } ->
      let base = [
        ("type", `String "enum");
        ("name", `String (Type_name.base_name enum_name));
        ("symbols", `List (List.map (fun s -> `String s) symbols));
      ] in
      let with_ns = match Type_name.namespace enum_name with
        | None -> base
        | Some ns -> base @ [("namespace", `String ns)]
      in
      let with_doc = match enum_doc with
        | None -> with_ns
        | Some doc -> with_ns @ [("doc", `String doc)]
      in
      let with_default = match enum_default with
        | None -> with_doc
        | Some def -> with_doc @ [("default", `String def)]
      in
      let with_aliases = match enum_aliases with
        | [] -> with_default
        | aliases -> with_default @ [("aliases", `List (List.map (fun a -> `String a) aliases))]
      in
      `Assoc with_aliases
  | Schema.Fixed { fixed_name; size; fixed_doc; fixed_aliases; fixed_logical } ->
      let base = [
        ("type", `String "fixed");
        ("name", `String (Type_name.base_name fixed_name));
        ("size", `Int size);
      ] in
      let with_ns = match Type_name.namespace fixed_name with
        | None -> base
        | Some ns -> base @ [("namespace", `String ns)]
      in
      let with_doc = match fixed_doc with
        | None -> with_ns
        | Some doc -> with_ns @ [("doc", `String doc)]
      in
      let with_logical = match fixed_logical with
        | None -> with_doc
        | Some lt -> with_doc @ [("logicalType", `String lt)]
      in
      let with_aliases = match fixed_aliases with
        | [] -> with_logical
        | aliases -> with_logical @ [("aliases", `List (List.map (fun a -> `String a) aliases))]
      in
      `Assoc with_aliases

let to_string schema =
  Fingerprint.to_canonical_json schema

let to_json schema =
  schema_to_json_full schema

let to_string_full schema =
  Yojson.Basic.to_string (schema_to_json_full schema)
