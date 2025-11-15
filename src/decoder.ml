(* TODO Many let rec functions in the library in general,
   should see how many we can replace with tail recursive
   versions. This will bite on large files. *)
let rec decode_value read_schema input =
  match read_schema with
  | Resolution.Null ->
      Input.read_null input;
      Value.Null

  | Resolution.Boolean ->
      Value.Boolean (Input.read_boolean input)

  | Resolution.Int ->
      Value.Int (Input.read_int input)

  | Resolution.Int_as_long ->
      Value.Long (Int64.of_int (Input.read_int input))

  | Resolution.Int_as_float ->
      Value.Float (float_of_int (Input.read_int input))

  | Resolution.Int_as_double ->
      Value.Double (float_of_int (Input.read_int input))

  | Resolution.Long ->
      Value.Long (Input.read_long input)

  | Resolution.Long_as_float ->
      Value.Float (Int64.to_float (Input.read_long input))

  | Resolution.Long_as_double ->
      Value.Double (Int64.to_float (Input.read_long input))

  | Resolution.Float ->
      Value.Float (Input.read_float input)

  | Resolution.Float_as_double ->
      Value.Double (Input.read_float input)

  | Resolution.Double ->
      Value.Double (Input.read_double input)

  | Resolution.Bytes ->
      Value.Bytes (Input.read_bytes input)

  | Resolution.String ->
      Value.String (Input.read_string input)

  | Resolution.Array elem_schema ->
      let rec read_blocks acc =
        let count = Input.read_long input in
        if count = 0L then
          List.rev acc
        else if count < 0L then
          let items = Array.init (Int64.to_int (Int64.neg count))
            (fun _ -> decode_value elem_schema input) in
          (read_blocks[@tailcall]) (items :: acc)
        else
          let items = Array.init (Int64.to_int count)
            (fun _ -> decode_value elem_schema input) in
          (read_blocks[@tailcall]) (items :: acc)
      in
      Value.Array (Array.concat (read_blocks []))

  | Resolution.Map elem_schema ->
      let rec read_blocks acc =
        let count = Input.read_long input in
        if count = 0L then
          List.rev acc
        else if count < 0L then
          let items = List.init (Int64.to_int (Int64.neg count))
            (fun _ ->
              let key = Input.read_string input in
              let value = decode_value elem_schema input in
              (key, value)
            ) in
          (read_blocks[@tailcall]) (List.rev_append items acc)
        else
          let items = List.init (Int64.to_int count)
            (fun _ ->
              let key = Input.read_string input in
              let value = decode_value elem_schema input in
              (key, value)
            ) in
          (read_blocks[@tailcall]) (List.rev_append items acc)
      in
      Value.Map (read_blocks [])

  | Resolution.Record { fields; defaults; _ } ->
      let decoded_fields = List.map (fun (field : Resolution.read_field) ->
        let value = decode_value field.field_schema input in
        (field.field_name, field.field_position, value)
      ) fields in

      let reader_fields = List.filter_map (fun (name, pos, value) ->
        match pos with
        | Some _ -> Some (name, value)
        | None -> None
      ) decoded_fields in

      (* Append defaults at the end, efficiently using tail-recursive reverse and append *)
      let with_defaults =
        match defaults with
        | [] -> reader_fields
        | _ ->
            let default_values = List.map (fun (_reader_pos, field_name, default) ->
              let default_value = Value.of_default default in
              (field_name, default_value)
            ) defaults in
            (* Since defaults is typically small, this @ is acceptable *)
            reader_fields @ default_values
      in

      Value.Record with_defaults

  | Resolution.Enum { symbols; symbol_map; _ } ->
      let writer_idx = Int64.to_int (Input.read_long input) in
      let reader_idx = symbol_map.(writer_idx) in
      let symbol = List.nth symbols reader_idx in
      Value.Enum (reader_idx, symbol)

  | Resolution.Union branches ->
      let writer_branch = Int64.to_int (Input.read_long input) in
      let (reader_branch, resolved_schema) = branches.(writer_branch) in
      let value = decode_value resolved_schema input in
      Value.Union (reader_branch, value)

  | Resolution.As_union (reader_branch, resolved_schema) ->
      let value = decode_value resolved_schema input in
      Value.Union (reader_branch, value)

  | Resolution.Fixed (_, size) ->
      Value.Fixed (Input.read_fixed input size)

  | Resolution.Named_type _ ->
     (* TODO Review our use of Exceptions, ideally want data types not strings *)
      failwith "Named_type should be resolved before decoding"

let decode_with_schemas reader_schema writer_schema bytes =
  match Resolution.resolve_schemas reader_schema writer_schema with
  | Ok read_schema ->
      let inp = Input.of_bytes bytes in
      Ok (decode_value read_schema inp)
  | Error mismatch ->
      Error mismatch
