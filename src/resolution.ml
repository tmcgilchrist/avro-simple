type read_field = {
  field_name: string;
  field_schema: read_schema;
  field_position: int option;
}

and read_schema =
  | Null
  | Boolean
  | Int
  | Int_as_long
  | Int_as_float
  | Int_as_double
  | Long
  | Long_as_float
  | Long_as_double
  | Float
  | Float_as_double
  | Double
  | Bytes
  | String
  | Array of read_schema
  | Map of read_schema
  | Record of {
      name: Type_name.t;
      fields: read_field list;
      defaults: (int * string * Schema.default) list;
    }
  | Enum of {
      name: Type_name.t;
      symbols: string list;
      symbol_map: int array;
    }
  | Union of (int * read_schema) array
  | As_union of int * read_schema
  | Fixed of Type_name.t * int
  | Named_type of Type_name.t

type mismatch =
  | Type_mismatch of Schema.t * Schema.t
  | Missing_field of Type_name.t * string
  | Field_mismatch of Type_name.t * string
  | Missing_union_branch of Type_name.t
  | Missing_symbol of string
  | Fixed_size_mismatch of Type_name.t * int * int
  | Named_type_unresolved of Type_name.t

let error_to_string = function
  | Type_mismatch (_reader, _writer) -> "Type mismatch"
  | Missing_field (record, field) ->
      Printf.sprintf "Missing field '%s' in record '%s'" field (Type_name.full_name record)
  | Field_mismatch (record, field) ->
      Printf.sprintf "Field '%s' type mismatch in record '%s'" field (Type_name.full_name record)
  | Missing_union_branch tn ->
      Printf.sprintf "Missing union branch: %s" (Type_name.full_name tn)
  | Missing_symbol sym ->
      Printf.sprintf "Missing enum symbol: %s" sym
  | Fixed_size_mismatch (tn, reader_size, writer_size) ->
      Printf.sprintf "Fixed type '%s' size mismatch: reader=%d, writer=%d"
        (Type_name.full_name tn) reader_size writer_size
  | Named_type_unresolved tn ->
      Printf.sprintf "Named type unresolved: %s" (Type_name.full_name tn)

type environment = (Type_name.t * Type_name.t) list

let rec deconflict env reader writer =

  (* TODO Make these basic_errors more descriptive, we loose the context that
     the Schema types don't match. *)
  let basic_error = Error (Type_mismatch (reader, writer)) in

  match reader with
  | Schema.Null ->
      (match writer with
      | Schema.Null -> Ok Null
      | _ -> basic_error)

  | Schema.Boolean ->
      (match writer with
      | Schema.Boolean -> Ok Boolean
      | _ -> basic_error)

  | Schema.Int _ ->
      (match writer with
      | Schema.Int _ -> Ok Int
      | _ -> basic_error)

  | Schema.Long _ ->
      (match writer with
      | Schema.Int _ -> Ok Int_as_long
      | Schema.Long _ -> Ok Long
      | _ -> basic_error)

  | Schema.Float ->
      (match writer with
      | Schema.Int _ -> Ok Int_as_float
      | Schema.Long _ -> Ok Long_as_float
      | Schema.Float -> Ok Float
      | _ -> basic_error)

  | Schema.Double ->
      (match writer with
      | Schema.Int _ -> Ok Int_as_double
      | Schema.Long _ -> Ok Long_as_double
      | Schema.Float -> Ok Float_as_double
      | Schema.Double -> Ok Double
      | _ -> basic_error)

  | Schema.Bytes _ ->
      (match writer with
      | Schema.Bytes _ -> Ok Bytes
      | Schema.String _ -> Ok Bytes
      | _ -> basic_error)

  | Schema.String _ ->
      (match writer with
      | Schema.Bytes _ -> Ok String
      | Schema.String _ -> Ok String
      | _ -> basic_error)

  | Schema.Array elem_reader ->
      (match writer with
      | Schema.Array elem_writer ->
          (match deconflict env elem_reader elem_writer with
          | Ok elem_resolved -> Ok (Array elem_resolved)
          | Error e -> Error e)
      | _ -> basic_error)

  | Schema.Map elem_reader ->
      (match writer with
      | Schema.Map elem_writer ->
          (match deconflict env elem_reader elem_writer with
          | Ok elem_resolved -> Ok (Map elem_resolved)
          | Error e -> Error e)
      | _ -> basic_error)

  | Schema.Fixed { fixed_name = reader_name; size = reader_size; fixed_aliases = reader_aliases; _ } ->
      (match writer with
      | Schema.Fixed { fixed_name = writer_name; size = writer_size; _ } ->
          if Type_name.compatible_names ~reader_name ~reader_aliases ~writer_name then
            if reader_size = writer_size then
              Ok (Fixed (reader_name, reader_size))
            else
              Error (Fixed_size_mismatch (reader_name, reader_size, writer_size))
          else
            basic_error
      | _ -> basic_error)

  | Schema.Record { name = reader_name; fields = reader_fields; record_aliases = reader_aliases; _ } ->
      (match writer with
      | Schema.Record { name = writer_name; fields = writer_fields; _ } ->
          if not (Type_name.compatible_names ~reader_name ~reader_aliases ~writer_name) then
            basic_error
          else
            let nested_env = (writer_name, reader_name) :: env in

            let reader_fields_indexed = List.mapi (fun i f -> (f, i)) reader_fields in

            let find_reader_field writer_field_name remaining =
              let rec find acc = function
                | [] -> None
                | (rf, idx) :: rest ->
                    if rf.Schema.field_name = writer_field_name ||
                       List.mem writer_field_name rf.Schema.field_aliases then
                      Some ((rf, idx), List.rev_append acc rest)
                    else
                      find ((rf, idx) :: acc) rest
              in
              find [] remaining
            in

            let rec process_writer_fields writer_flds remaining_reader acc_fields =
              match writer_flds with
              | [] ->
                  let rec collect_defaults remaining acc_defaults =
                    match remaining with
                    | [] -> Ok (List.rev acc_defaults)
                    | (rf, idx) :: rest ->
                        (match rf.Schema.field_default with
                        | Some default ->
                            collect_defaults rest ((idx, rf.Schema.field_name, default) :: acc_defaults)
                        | None ->
                            Error (Missing_field (reader_name, rf.Schema.field_name)))
                  in
                  (match collect_defaults remaining_reader [] with
                  | Ok defaults ->
                      Ok (Record {
                        name = reader_name;
                        fields = List.rev acc_fields;
                        defaults;
                      })
                  | Error e -> Error e)

              | wf :: rest_writer ->
                  (match find_reader_field wf.Schema.field_name remaining_reader with
                  | Some ((rf, reader_pos), remaining') ->
                      (match deconflict nested_env rf.Schema.field_type wf.Schema.field_type with
                      | Ok resolved_type ->
                          let read_field = {
                            field_name = rf.Schema.field_name;
                            field_schema = resolved_type;
                            field_position = Some reader_pos;
                          } in
                          process_writer_fields rest_writer remaining' (read_field :: acc_fields)
                      | Error _ ->
                          Error (Field_mismatch (reader_name, wf.Schema.field_name)))

                  | None ->
                      (match deconflict nested_env wf.Schema.field_type wf.Schema.field_type with
                      | Ok resolved_type ->
                          let read_field = {
                            field_name = wf.Schema.field_name;
                            field_schema = resolved_type;
                            field_position = None;
                          } in
                          process_writer_fields rest_writer remaining_reader (read_field :: acc_fields)
                      | Error e -> Error e))
            in
            process_writer_fields writer_fields reader_fields_indexed []

      | _ -> basic_error)

  | Schema.Enum { enum_name = reader_name; symbols = reader_symbols; enum_default; enum_aliases = reader_aliases; _ } ->
      (match writer with
      | Schema.Enum { enum_name = writer_name; symbols = writer_symbols; _ } ->
          if not (Type_name.compatible_names ~reader_name ~reader_aliases ~writer_name) then
            basic_error
          else
            let default_idx = match enum_default with
              | None -> None
              | Some default_sym ->
                  let rec find_index sym lst idx =
                    match lst with
                    | [] -> None
                    | s :: rest ->
                        if s = sym then Some idx
                        else find_index sym rest (idx + 1)
                  in
                  find_index default_sym reader_symbols 0
            in

            let map_symbol writer_sym =
              let rec find_index sym lst idx =
                match lst with
                | [] -> None
                | s :: rest ->
                    if s = sym then Some idx
                    else find_index sym rest (idx + 1)
              in
              match find_index writer_sym reader_symbols 0 with
              | Some reader_idx -> Ok reader_idx
              | None ->
                  (match default_idx with
                  | Some idx -> Ok idx
                  | None -> Error (Missing_symbol writer_sym))
            in
            let rec map_all symbols acc count =
              match symbols with
              | [] ->
                  let result = Array.make count 0 in
                  List.iteri (fun i idx -> result.(count - 1 - i) <- idx) acc;
                  Ok result
              | sym :: rest ->
                  (match map_symbol sym with
                  | Ok idx -> map_all rest (idx :: acc) (count + 1)
                  | Error e -> Error e)
            in
            (match map_all writer_symbols [] 0 with
            | Ok symbol_map ->
                Ok (Enum { name = reader_name; symbols = reader_symbols; symbol_map })
            | Error e -> Error e)

      | _ -> basic_error)

  | Schema.Union reader_branches ->
      (match writer with
      | Schema.Union writer_branches ->
          let rec resolve_branches branches acc count =
            match branches with
            | [] ->
                let result = Array.make count (0, Null) in
                List.iteri (fun i item -> result.(count - 1 - i) <- item) acc;
                Ok result
            | writer_branch :: rest ->
                (match find_union_branch reader_branches writer_branch with
                | Some (reader_idx, resolved) ->
                    resolve_branches rest ((reader_idx, resolved) :: acc) (count + 1)
                | None ->
                    Error (Missing_union_branch (Type_name.simple "union")))
          in
          (match resolve_branches writer_branches [] 0 with
          | Ok resolved_array -> Ok (Union resolved_array)
          | Error e -> Error e)

      | singular ->
          (match find_union_branch reader_branches singular with
          | Some (reader_idx, resolved) ->
              Ok (As_union (reader_idx, resolved))
          | None ->
              basic_error))

and find_union_branch reader_branches writer_type =
  let rec try_branches branches idx =
    match branches with
    | [] -> None
    | reader_branch :: rest ->
        (match deconflict [] reader_branch writer_type with
        | Ok resolved -> Some (idx, resolved)
        | Error _ -> try_branches rest (idx + 1))
  in
  try_branches reader_branches 0

let resolve_schemas reader writer =
  deconflict [] reader writer
