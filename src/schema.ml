type t =
  | Null
  | Boolean
  | Int of string option
  | Long of string option
  | Float
  | Double
  | Bytes of string option
  | String of string option
  | Array of t
  | Map of t
  | Record of record_schema
  | Enum of enum_schema
  | Union of t list
  | Fixed of fixed_schema

and record_schema = {
  name: Type_name.t;
  fields: field list;
  record_doc: string option;
  record_aliases: string list;
}

and field = {
  field_name: string;
  field_type: t;
  field_doc: string option;
  field_default: default option;
  field_aliases: string list;
}

and enum_schema = {
  enum_name: Type_name.t;
  symbols: string list;
  enum_doc: string option;
  enum_default: string option;
  enum_aliases: string list;
}

and fixed_schema = {
  fixed_name: Type_name.t;
  size: int;
  fixed_doc: string option;
  fixed_aliases: string list;
  fixed_logical: string option;
}

and default =
  | Null_default
  | Bool_default of bool
  | Int_default of int
  | Long_default of int64
  | Float_default of float
  | Double_default of float
  | Bytes_default of bytes
  | String_default of string
  | Enum_default of string
  | Array_default of default list
  | Map_default of (string * default) list
  | Union_default of int * default

let has_duplicate_fields fields =
  let rec check_dups seen = function
    | [] -> None
    | f :: rest ->
        if List.mem f.field_name seen then
          Some f.field_name
        else
          check_dups (f.field_name :: seen) rest
  in
  check_dups [] fields

let has_duplicate_symbols symbols =
  let rec check_dups seen = function
    | [] -> None
    | s :: rest ->
        if List.mem s seen then
          Some s
        else
          check_dups (s :: seen) rest
  in
  check_dups [] symbols

let is_valid_name name =
  if String.length name = 0 then false
  else
    let first_char = name.[0] in
    let valid_first = (first_char >= 'a' && first_char <= 'z') ||
                     (first_char >= 'A' && first_char <= 'Z') ||
                     first_char = '_' in
    if not valid_first then false
    else
      String.for_all (fun c ->
        (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') ||
        c = '_'
      ) name

let rec validate = function
  | Null | Boolean | Int _ | Long _ | Float | Double | Bytes _ | String _ -> Ok ()

  | Array schema ->
      validate schema

  | Map schema ->
      validate schema

  | Union schemas ->
      if List.length schemas < 2 then
        Error "Union must have at least 2 branches"
      else if List.exists (function Union _ -> true | _ -> false) schemas then
        Error "Union cannot directly contain another union"
      else
        let rec has_duplicate_types seen = function
          | [] -> None
          | s :: rest ->
              let type_key = match s with
                | Null -> "null"
                | Boolean -> "boolean"
                | Int _ -> "int"
                | Long _ -> "long"
                | Float -> "float"
                | Double -> "double"
                | Bytes _ -> "bytes"
                | String _ -> "string"
                | Array _ -> "array"
                | Map _ -> "map"
                | Record { name; _ } -> "record:" ^ Type_name.full_name name
                | Enum { enum_name; _ } -> "enum:" ^ Type_name.full_name enum_name
                | Fixed { fixed_name; _ } -> "fixed:" ^ Type_name.full_name fixed_name
                | Union _ -> "union"
              in
              if List.mem type_key seen then
                Some type_key
              else
                has_duplicate_types (type_key :: seen) rest
        in
        begin match has_duplicate_types [] schemas with
        | Some dup ->
            Error (Printf.sprintf "Union has duplicate type: %s" dup)
        | None ->
            List.fold_left (fun acc s ->
              match acc with
              | Error _ -> acc
              | Ok () -> validate s
            ) (Ok ()) schemas
        end

  | Record r ->
      if List.length r.fields = 0 then
        Error "Record must have at least one field"
      else begin
        match has_duplicate_fields r.fields with
        | Some dup ->
            Error (Printf.sprintf "Record has duplicate field name: %s" dup)
        | None ->
            let invalid_field = List.find_opt
              (fun f -> not (is_valid_name f.field_name))
              r.fields in
            begin match invalid_field with
            | Some f ->
                Error (Printf.sprintf "Invalid field name: %s" f.field_name)
            | None ->
                List.fold_left (fun acc f ->
                  match acc with
                  | Error _ -> acc
                  | Ok () -> validate f.field_type
                ) (Ok ()) r.fields
            end
      end

  | Enum e ->
      if List.length e.symbols = 0 then
        Error "Enum must have at least one symbol"
      else begin
        match has_duplicate_symbols e.symbols with
        | Some dup ->
            Error (Printf.sprintf "Enum has duplicate symbol: %s" dup)
        | None ->
            let invalid_symbol = List.find_opt
              (fun s -> not (is_valid_name s))
              e.symbols in
            match invalid_symbol with
            | Some s ->
                Error (Printf.sprintf "Invalid enum symbol name: %s" s)
            | None ->
                Ok ()
      end

  | Fixed f ->
      if f.size <= 0 then
        Error "Fixed type size must be positive"
      else
        Ok ()

let validate_no_name_redefinition schema =
  let rec collect_names seen_schemas name_map = function
    | (Null | Boolean | Int _ | Long _ | Float | Double | Bytes _ | String _) as s ->
        if List.memq s seen_schemas then
          Ok (seen_schemas, name_map)
        else
          Ok (s :: seen_schemas, name_map)

    | (Array _ | Map _ | Union _ | Record _ | Enum _ | Fixed _) as s ->
        if List.memq s seen_schemas then
          Ok (seen_schemas, name_map)
        else
          let seen_schemas' = s :: seen_schemas in
          match s with
          | Array s' | Map s' ->
              collect_names seen_schemas' name_map s'

          | Union schemas ->
              List.fold_left (fun acc_result schema ->
                match acc_result with
                | Error _ -> acc_result
                | Ok (seen, names) -> collect_names seen names schema
              ) (Ok (seen_schemas', name_map)) schemas

          | Record { name; fields; _ } ->
              let full_name = Type_name.full_name name in
              if List.mem full_name name_map then
                Error (Printf.sprintf "Name redefined: %s" full_name)
              else
                let name_map' = full_name :: name_map in
                List.fold_left (fun acc_result f ->
                  match acc_result with
                  | Error _ -> acc_result
                  | Ok (seen, names) -> collect_names seen names f.field_type
                ) (Ok (seen_schemas', name_map')) fields

          | Enum { enum_name; _ } ->
              let full_name = Type_name.full_name enum_name in
              if List.mem full_name name_map then
                Error (Printf.sprintf "Name redefined: %s" full_name)
              else
                Ok (seen_schemas', full_name :: name_map)

          | Fixed { fixed_name; _ } ->
              let full_name = Type_name.full_name fixed_name in
              if List.mem full_name name_map then
                Error (Printf.sprintf "Name redefined: %s" full_name)
              else
                Ok (seen_schemas', full_name :: name_map)

          | _ -> Ok (seen_schemas', name_map)
  in
  match collect_names [] [] schema with
  | Ok _ -> Ok ()
  | Error e -> Error e

let validate_schema schema =
  match validate schema with
  | Error e -> Error e
  | Ok () -> validate_no_name_redefinition schema

let with_logical_type logical_type schema =
  match schema with
  | Int _ -> Int (Some logical_type)
  | Long _ -> Long (Some logical_type)
  | Bytes _ -> Bytes (Some logical_type)
  | String _ -> String (Some logical_type)
  | Fixed fs -> Fixed { fs with fixed_logical = Some logical_type }
  | _ -> schema

let to_json _schema = "{}"

let of_json _json = Error "Not implemented"
