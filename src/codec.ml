type 'a t = {
  schema: Schema.t;
  encode: 'a -> Output.t -> unit;
  decode: Input.t -> 'a;
}

let null = {
  schema = Schema.Null;
  encode = (fun () out -> Output.write_null out ());
  decode = (fun inp -> Input.read_null inp);
}

let boolean = {
  schema = Schema.Boolean;
  encode = (fun b out -> Output.write_boolean out b);
  decode = (fun inp -> Input.read_boolean inp);
}

let int = {
  schema = Schema.Int None;
  encode = (fun i out -> Output.write_int out i);
  decode = (fun inp -> Input.read_int inp);
}

let long = {
  schema = Schema.Long None;
  encode = (fun l out -> Output.write_long out l);
  decode = (fun inp -> Input.read_long inp);
}

let float = {
  schema = Schema.Float;
  encode = (fun f out -> Output.write_float out f);
  decode = (fun inp -> Input.read_float inp);
}

let double = {
  schema = Schema.Double;
  encode = (fun d out -> Output.write_double out d);
  decode = (fun inp -> Input.read_double inp);
}

let bytes = {
  schema = Schema.Bytes None;
  encode = (fun b out -> Output.write_bytes out b);
  decode = (fun inp -> Input.read_bytes inp);
}

let string = {
  schema = Schema.String None;
  encode = (fun s out -> Output.write_string out s);
  decode = (fun inp -> Input.read_string inp);
}

(* TODO Warning on un-used variable *)
(* TODO In general reduce type conversions between List, Array, Bytes, String. *)
let fixed ?(name = "fixed") size = {
  schema = Schema.Fixed {
    fixed_name = Type_name.simple name;
    size;
    fixed_doc = None;
    fixed_aliases = [];
    fixed_logical = None;
  };
  encode = (fun bytes out ->
    if Bytes.length bytes <> size then
      failwith (Printf.sprintf "Fixed type size mismatch: expected %d bytes, got %d"
        size (Bytes.length bytes));
    Output.write_fixed out bytes
  );
  decode = (fun inp -> Input.read_fixed inp size);
}

let array codec = {
  schema = Schema.Array codec.schema;
  encode = (fun arr out ->
    let len = Array.length arr in
    if len = 0 then
      Output.write_long out 0L
    else begin
      Output.write_long out (Int64.of_int len);
      Array.iter (fun elem -> codec.encode elem out) arr;
      Output.write_long out 0L
    end
  );
  decode = (fun inp ->
    (* Accumulate arrays from blocks, then concatenate at end *)
    let rec read_blocks acc =
      let count = Input.read_long inp in
      if count = 0L then
        List.rev acc
      else if count < 0L then
        let _size = Input.read_long inp in
        let items = Array.init (Int64.to_int (Int64.neg count))
          (fun _ -> codec.decode inp) in
        (read_blocks[@tailcall]) (items :: acc)
      else
        let items = Array.init (Int64.to_int count)
          (fun _ -> codec.decode inp) in
        (read_blocks[@tailcall]) (items :: acc)
    in
    let arrays = read_blocks [] in
    (* Concatenate all arrays efficiently *)
    Array.concat arrays
  );
}

let map codec = {
  schema = Schema.Map codec.schema;
  encode = (fun pairs out ->
    let len = List.length pairs in
    if len = 0 then
      Output.write_long out 0L
    else begin
      Output.write_long out (Int64.of_int len);
      List.iter (fun (key, value) ->
        Output.write_string out key;
        codec.encode value out
      ) pairs;
      Output.write_long out 0L
    end
  );
  decode = (fun inp ->
    let rec read_blocks acc =
      let count = Input.read_long inp in
      if count = 0L then
        List.rev acc
      else if count < 0L then
        let _size = Input.read_long inp in
        let items = List.init (Int64.to_int (Int64.neg count))
          (fun _ ->
            let key = Input.read_string inp in
            let value = codec.decode inp in
            (key, value)
          ) in
        read_blocks (List.rev_append items acc)
      else
        let items = List.init (Int64.to_int count)
          (fun _ ->
            let key = Input.read_string inp in
            let value = codec.decode inp in
            (key, value)
          ) in
        read_blocks (List.rev_append items acc)
    in
    read_blocks []
  );
}

let union codecs = {
  schema = Schema.Union (List.map (fun c -> c.schema) codecs);
  encode = (fun (branch, value) out ->
    Output.write_long out (Int64.of_int branch);
    (List.nth codecs branch).encode value out
  );
  decode = (fun inp ->
    let branch = Int64.to_int (Input.read_long inp) in
    let value = (List.nth codecs branch).decode inp in
    (branch, value)
  );
}

let option codec = {
  schema = Schema.Union [Schema.Null; codec.schema];
  encode = (fun opt out ->
    match opt with
    | None ->
        Output.write_long out 0L;
        Output.write_null out ()
    | Some value ->
        Output.write_long out 1L;
        codec.encode value out
  );
  decode = (fun inp ->
    let branch = Int64.to_int (Input.read_long inp) in
    match branch with
    | 0 ->
        Input.read_null inp;
        None
    | 1 ->
        Some (codec.decode inp)
    | _ ->
        failwith "Invalid union branch for option"
  );
}

type ('record, 'constructor) builder = {
  type_name: Type_name.t;
  constructor: 'constructor;
  fields_rev: Schema.field list;
  encode: 'record -> Output.t -> unit;
  decode: Input.t -> 'constructor;
}

let record type_name constructor = {
  type_name;
  constructor;
  fields_rev = [];
  encode = (fun _ _ -> ());
  decode = (fun _ -> constructor);
}

let field field_name field_codec getter builder =
  let field_schema = {
    Schema.field_name;
    Schema.field_type = field_codec.schema;
    Schema.field_doc = None;
    Schema.field_default = None;
    Schema.field_aliases = [];
  } in

  let new_encode record out =
    builder.encode record out;
    field_codec.encode (getter record) out
  in

  let new_decode inp =
    let partial = builder.decode inp in
    let value = field_codec.decode inp in
    partial value
  in

  {
    type_name = builder.type_name;
    constructor = (Obj.magic ());
    fields_rev = field_schema :: builder.fields_rev;
    encode = new_encode;
    decode = new_decode;
  }

let field_opt field_name field_codec getter builder =
  let option_codec = option field_codec in
  let field_schema = {
    Schema.field_name;
    Schema.field_type = option_codec.schema;
    Schema.field_doc = None;
    Schema.field_default = Some Schema.Null_default;
    Schema.field_aliases = [];
  } in

  let new_encode record out =
    builder.encode record out;
    option_codec.encode (getter record) out
  in

  let new_decode inp =
    let partial = builder.decode inp in
    let value = option_codec.decode inp in
    partial value
  in

  {
    type_name = builder.type_name;
    constructor = (Obj.magic ());
    fields_rev = field_schema :: builder.fields_rev;
    encode = new_encode;
    decode = new_decode;
  }

let finish builder =
  let schema = Schema.Record {
    name = builder.type_name;
    fields = List.rev builder.fields_rev;
    record_doc = None;
    record_aliases = [];
  } in

  {
    schema;
    encode = builder.encode;
    decode = builder.decode;
  }

let encode_to_bytes (codec : 'a t) (value : 'a) : bytes =
  let out = Output.create () in
  codec.encode value out;
  Output.to_bytes out

let decode_from_bytes (codec : 'a t) (bytes : bytes) : 'a =
  let inp = Input.of_bytes bytes in
  codec.decode inp

let encode_to_string (codec : 'a t) (value : 'a) : string =
  let out = Output.create () in
  codec.encode value out;
  Output.contents out

let decode_from_string (codec : 'a t) (str : string) : 'a =
  let inp = Input.of_string str in
  codec.decode inp

let recursive (f : 'a t -> 'a t) : 'a t =
  (* Create mutable references for the codec components *)
  let schema_ref = ref Schema.Null in
  let encode_ref = ref (fun _ _ -> ()) in
  let decode_ref = ref (fun _ -> failwith "recursive codec not initialized") in

  (* Create a placeholder codec that dereferences at call time *)
  let self = {
    schema = Schema.Null;  (* Will be updated after construction *)
    encode = (fun v out -> !encode_ref v out);
    decode = (fun inp -> !decode_ref inp);
  } in

  (* Build the actual codec using the placeholder *)
  let actual = f self in

  (* Backpatch the references *)
  schema_ref := actual.schema;
  encode_ref := actual.encode;
  decode_ref := actual.decode;

  (* Return a codec with the correct schema but thunked encode/decode *)
  {
    schema = actual.schema;
    encode = actual.encode;
    decode = actual.decode;
  }
