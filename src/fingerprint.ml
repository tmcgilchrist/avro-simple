let rec to_canonical_json schema =
  match schema with
  | Schema.Null -> "\"null\""
  | Schema.Boolean -> "\"boolean\""
  | Schema.Int _ -> "\"int\""
  | Schema.Long _ -> "\"long\""
  | Schema.Float -> "\"float\""
  | Schema.Double -> "\"double\""
  | Schema.Bytes _ -> "\"bytes\""
  | Schema.String _ -> "\"string\""

  | Schema.Array item_type ->
      Printf.sprintf "{\"type\":\"array\",\"items\":%s}"
        (to_canonical_json item_type)

  | Schema.Map value_type ->
      Printf.sprintf "{\"type\":\"map\",\"values\":%s}"
        (to_canonical_json value_type)

  | Schema.Record { name; fields; _ } ->
      let fields_json = List.map (fun (f : Schema.field) ->
        Printf.sprintf "{\"name\":\"%s\",\"type\":%s}"
          f.field_name
          (to_canonical_json f.field_type)
      ) fields |> String.concat "," in
      Printf.sprintf "{\"name\":\"%s\",\"type\":\"record\",\"fields\":[%s]}"
        (Type_name.full_name name)
        fields_json

  | Schema.Enum { enum_name; symbols; _ } ->
      let symbols_json = List.map (Printf.sprintf "\"%s\"") symbols
                         |> String.concat "," in
      Printf.sprintf "{\"name\":\"%s\",\"type\":\"enum\",\"symbols\":[%s]}"
        (Type_name.full_name enum_name)
        symbols_json

  | Schema.Union branches ->
      let branches_json = List.map to_canonical_json branches
                          |> String.concat "," in
      Printf.sprintf "[%s]" branches_json

  | Schema.Fixed { fixed_name; size; _ } ->
      Printf.sprintf "{\"name\":\"%s\",\"type\":\"fixed\",\"size\":%d}"
        (Type_name.full_name fixed_name)
        size

let crc64_poly = 0xC96C5795D7870F42L

let crc64_table =
  let table = Array.make 256 0L in
  for i = 0 to 255 do
    let rec compute_entry crc bit =
      if bit >= 8 then crc
      else
        let crc' =
          if Int64.(logand crc 1L) = 1L then
            Int64.(logxor (shift_right_logical crc 1) crc64_poly)
          else
            Int64.shift_right_logical crc 1
        in
        compute_entry crc' (bit + 1)
    in
    table.(i) <- compute_entry (Int64.of_int i) 0
  done;
  table

let crc64_of_string str =
  let len = String.length str in
  let rec loop i crc =
    if i >= len then crc
    else
      let byte = Char.code str.[i] in
      let index = Int64.(to_int (logand (logxor crc (of_int byte)) 0xFFL)) in
      let crc' = Int64.(logxor (shift_right_logical crc 8) crc64_table.(index)) in
      loop (i + 1) crc'
  in
  loop 0 0xFFFFFFFFFFFFFFFFL

let crc64 schema =
  let canonical = to_canonical_json schema in
  crc64_of_string canonical

let rabin_fingerprint schema =
  crc64 schema
