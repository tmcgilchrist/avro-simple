(** Logical type conversion helpers implementation *)

(* Helper: days between two dates *)
let days_since_epoch (year, month, day) =
  (* Unix epoch is 1970-01-01 *)
  let epoch_year = 1970 in

  (* Validate date *)
  if year < 1 || month < 1 || month > 12 || day < 1 || day > 31 then
    invalid_arg "Invalid date";

  (* Calculate days *)
  let is_leap_year y =
    (y mod 4 = 0 && y mod 100 <> 0) || (y mod 400 = 0)
  in

  let days_in_month y m =
    match m with
    | 1 | 3 | 5 | 7 | 8 | 10 | 12 -> 31
    | 4 | 6 | 9 | 11 -> 30
    | 2 -> if is_leap_year y then 29 else 28
    | _ -> invalid_arg "Invalid month"
  in

  (* Validate day for month *)
  if day > days_in_month year month then
    invalid_arg "Invalid day for month";

  (* Count days from epoch to this date *)
  let days_in_year y =
    if is_leap_year y then 366 else 365
  in

  let rec year_days y acc =
    if y >= year then acc
    else year_days (y + 1) (acc + days_in_year y)
  in

  let rec year_days_back y acc =
    if y <= year then acc
    else year_days_back (y - 1) (acc - days_in_year (y - 1))
  in

  let year_offset =
    if year >= epoch_year then
      year_days epoch_year 0
    else
      year_days_back epoch_year 0
  in

  let month_offset =
    let rec loop m acc =
      if m >= month then acc
      else loop (m + 1) (acc + days_in_month year m)
    in
    loop 1 0
  in

  year_offset + month_offset + day - 1

let date_from_days days =
  (* Convert days since epoch back to (year, month, day) *)
  let epoch_year = 1970 in

  let is_leap_year y =
    (y mod 4 = 0 && y mod 100 <> 0) || (y mod 400 = 0)
  in

  let days_in_year y =
    if is_leap_year y then 366 else 365
  in

  let days_in_month y m =
    match m with
    | 1 | 3 | 5 | 7 | 8 | 10 | 12 -> 31
    | 4 | 6 | 9 | 11 -> 30
    | 2 -> if is_leap_year y then 29 else 28
    | _ -> invalid_arg "Invalid month"
  in

  (* Find the year *)
  let rec find_year y remaining_days =
    let days_this_year = days_in_year y in
    if remaining_days < days_this_year then
      (y, remaining_days)
    else
      find_year (y + 1) (remaining_days - days_this_year)
  in

  let rec find_year_back y remaining_days =
    if remaining_days >= 0 then
      (y, remaining_days)
    else
      let prev_year = y - 1 in
      find_year_back prev_year (remaining_days + days_in_year prev_year)
  in

  let (year, days_in_year) =
    if days >= 0 then
      find_year epoch_year days
    else
      find_year_back epoch_year days
  in

  (* Find the month *)
  let rec find_month m remaining_days =
    let days_this_month = days_in_month year m in
    if remaining_days < days_this_month then
      (m, remaining_days + 1)  (* +1 because days are 1-indexed *)
    else if m = 12 then
      (12, days_this_month)  (* Should not happen with correct calculation *)
    else
      find_month (m + 1) (remaining_days - days_this_month)
  in

  let (month, day) = find_month 1 days_in_year in
  (year, month, day)

let date = {
  Codec.schema = Schema.Int (Some "date");
  encode = (fun (y, m, d) out ->
    let days = days_since_epoch (y, m, d) in
    Output.write_int out days);
  decode = (fun inp ->
    let days = Input.read_int inp in
    date_from_days days);
}

(* Time helpers *)
let time_to_millis (hour, minute, second, milli) =
  if hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
     second < 0 || second > 59 || milli < 0 || milli > 999 then
    invalid_arg "Invalid time";
  (((hour * 60 + minute) * 60 + second) * 1000 + milli)

let time_from_millis ms =
  if ms < 0 || ms >= 86400000 then
    invalid_arg "Time milliseconds out of range";
  let second_ms = ms / 1000 in
  let milli = ms mod 1000 in
  let minute_s = second_ms / 60 in
  let second = second_ms mod 60 in
  let hour = minute_s / 60 in
  let minute = minute_s mod 60 in
  (hour, minute, second, milli)

let time_millis = {
  Codec.schema = Schema.Int (Some "time-millis");
  encode = (fun time out ->
    let ms = time_to_millis time in
    Output.write_int out ms);
  decode = (fun inp ->
    let ms = Input.read_int inp in
    time_from_millis ms);
}

let time_to_micros (hour, minute, second, micro) =
  if hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
     second < 0 || second > 59 || micro < 0 || micro > 999999 then
    invalid_arg "Invalid time";
  Int64.(add (mul (of_int ((hour * 60 + minute) * 60 + second)) 1000000L)
              (of_int micro))

let time_from_micros us =
  if us < 0L || us >= 86400000000L then
    invalid_arg "Time microseconds out of range";
  let open Int64 in
  let second_us = div us 1000000L in
  let micro = to_int (rem us 1000000L) in
  let minute_s = div second_us 60L in
  let second = to_int (rem second_us 60L) in
  let hour = to_int (div minute_s 60L) in
  let minute = to_int (rem minute_s 60L) in
  (hour, minute, second, micro)

let time_micros = {
  Codec.schema = Schema.Long (Some "time-micros");
  encode = (fun time out ->
    let us = time_to_micros time in
    Output.write_long out us);
  decode = (fun inp ->
    let us = Input.read_long inp in
    time_from_micros us);
}

(* Timestamp codecs - these are just int64 with schema annotations *)
let timestamp_millis = {
  Codec.schema = Schema.Long (Some "timestamp-millis");
  encode = (fun ts out -> Output.write_long out ts);
  decode = (fun inp -> Input.read_long inp);
}

let timestamp_micros = {
  Codec.schema = Schema.Long (Some "timestamp-micros");
  encode = (fun ts out -> Output.write_long out ts);
  decode = (fun inp -> Input.read_long inp);
}

let local_timestamp_millis = {
  Codec.schema = Schema.Long (Some "local-timestamp-millis");
  encode = (fun ts out -> Output.write_long out ts);
  decode = (fun inp -> Input.read_long inp);
}

let local_timestamp_micros = {
  Codec.schema = Schema.Long (Some "local-timestamp-micros");
  encode = (fun ts out -> Output.write_long out ts);
  decode = (fun inp -> Input.read_long inp);
}

(* Decimal conversion helpers *)
let z_to_bytes z =
  (* Convert Z.t to two's complement big-endian bytes *)
  if Z.equal z Z.zero then
    Bytes.make 1 '\000'
  else if Z.gt z Z.zero then
    (* Positive number *)
    let rec to_bytes_list n acc =
      if Z.equal n Z.zero then acc
      else
        let byte = Z.to_int (Z.logand n (Z.of_int 0xFF)) in
        to_bytes_list (Z.shift_right n 8) (byte :: acc)
    in
    let bytes_list = to_bytes_list z [] in
    (* Check if high bit is set, add 0x00 prefix if needed *)
    let bytes_list' =
      match bytes_list with
      | [] -> [0]
      | hd :: _ when hd >= 128 -> 0 :: bytes_list
      | _ -> bytes_list
    in
    let len = List.length bytes_list' in
    let bytes = Bytes.create len in
    List.iteri (fun i b -> Bytes.set bytes i (Char.chr b)) bytes_list';
    bytes
  else
    (* Negative number - two's complement *)
    let pos = Z.neg z in
    let rec to_bytes_list n acc =
      if Z.equal n Z.zero then acc
      else
        let byte = Z.to_int (Z.logand n (Z.of_int 0xFF)) in
        to_bytes_list (Z.shift_right n 8) (byte :: acc)
    in
    let bytes_list = to_bytes_list pos [] in
    (* Compute two's complement *)
    let rec twos_complement lst carry =
      match lst with
      | [] -> if carry = 1 then [1] else []
      | b :: rest ->
          let inverted = 0xFF - b in
          let result = inverted + carry in
          (result land 0xFF) :: twos_complement rest (result lsr 8)
    in
    let rev_list = List.rev bytes_list in
    let tc_list = twos_complement rev_list 1 in
    let tc_list' = List.rev tc_list in
    (* Ensure high bit is set for negative *)
    let tc_list'' =
      match tc_list' with
      | [] -> [0xFF]
      | hd :: _ when hd < 128 -> 0xFF :: tc_list'
      | _ -> tc_list'
    in
    let len = List.length tc_list'' in
    let bytes = Bytes.create len in
    List.iteri (fun i b -> Bytes.set bytes i (Char.chr b)) tc_list'';
    bytes

let z_from_bytes bytes =
  (* Convert two's complement big-endian bytes to Z.t *)
  let len = Bytes.length bytes in
  if len = 0 then Z.zero
  else
    let first_byte = Char.code (Bytes.get bytes 0) in
    let is_negative = first_byte >= 128 in

    if is_negative then
      (* Negative number - undo two's complement *)
      let rec from_bytes i acc =
        if i >= len then acc
        else
          let byte = Char.code (Bytes.get bytes i) in
          from_bytes (i + 1) Z.(logor (shift_left acc 8) (of_int byte))
      in
      let value = from_bytes 0 Z.zero in
      (* Undo two's complement: flip bits and add 1 *)
      let bits = len * 8 in
      let mask = Z.(sub (shift_left one bits) one) in
      let inverted = Z.logxor value mask in
      Z.(neg (succ inverted))
    else
      (* Positive number *)
      let rec from_bytes i acc =
        if i >= len then acc
        else
          let byte = Char.code (Bytes.get bytes i) in
          from_bytes (i + 1) Z.(logor (shift_left acc 8) (of_int byte))
      in
      from_bytes 0 Z.zero

let decimal ~precision ~scale =
  if precision <= 0 then
    invalid_arg "Decimal precision must be positive";
  if scale < 0 then
    invalid_arg "Decimal scale must be non-negative";

  {
    Codec.schema = Schema.Bytes (Some "decimal");
    encode = (fun z out ->
      let bytes = z_to_bytes z in
      Output.write_bytes out bytes);
    decode = (fun inp ->
      let bytes = Input.read_bytes inp in
      z_from_bytes bytes);
  }

let decimal_fixed ~precision ~scale ~size =
  if precision <= 0 then
    invalid_arg "Decimal precision must be positive";
  if scale < 0 then
    invalid_arg "Decimal scale must be non-negative";
  if size <= 0 then
    invalid_arg "Decimal fixed size must be positive";

  (* Verify size is sufficient for precision *)
  (* Each byte can represent ~2.4 decimal digits (log10(256) ≈ 2.408) *)
  let max_digits = int_of_float (float_of_int size *. 2.408) in
  if max_digits < precision then
    invalid_arg (Printf.sprintf
      "Decimal fixed size %d bytes insufficient for precision %d (max ~%d digits)"
      size precision max_digits);

  let schema = Schema.Fixed {
    fixed_name = Type_name.simple (Printf.sprintf "decimal_%d" size);
    size;
    fixed_doc = Some (Printf.sprintf "Decimal(precision=%d, scale=%d)" precision scale);
    fixed_aliases = [];
    fixed_logical = Some "decimal";
  } in

  let encode_fixed z =
    let bytes = z_to_bytes z in
    let len = Bytes.length bytes in
    if len > size then
      invalid_arg (Printf.sprintf
        "Decimal value too large for fixed size %d (requires %d bytes)" size len);
    if len = size then
      bytes
    else
      (* Pad with sign extension *)
      let padded = Bytes.create size in
      let pad_byte = if Z.lt z Z.zero then '\xFF' else '\x00' in
      Bytes.fill padded 0 (size - len) pad_byte;
      Bytes.blit bytes 0 padded (size - len) len;
      padded
  in

  {
    Codec.schema = schema;
    encode = (fun z out ->
      let bytes = encode_fixed z in
      Output.write_fixed out bytes);
    decode = (fun inp ->
      let bytes = Input.read_fixed inp size in
      z_from_bytes bytes);
  }

let uuid = {
  Codec.schema = Schema.String (Some "uuid");
  encode = (fun u out ->
    let s = Uuidm.to_string u in
    Output.write_string out s);
  decode = (fun inp ->
    let s = Input.read_string inp in
    match Uuidm.of_string s with
    | Some u -> u
    | None -> invalid_arg ("Invalid UUID string: " ^ s));
}

type duration = {
  months: int;
  days: int;
  millis: int;
}

let duration =
  let schema = Schema.Fixed {
    fixed_name = Type_name.simple "duration";
    size = 12;
    fixed_doc = Some "Duration with months, days, and milliseconds";
    fixed_aliases = [];
    fixed_logical = Some "duration";
  } in

  let encode_duration d =
    if d.months < 0 || d.days < 0 || d.millis < 0 then
      invalid_arg "Duration components must be non-negative";

    let bytes = Bytes.create 12 in

    (* Encode as little-endian unsigned 32-bit integers *)
    let set_le32 offset value =
      Bytes.set bytes offset (Char.chr (value land 0xFF));
      Bytes.set bytes (offset + 1) (Char.chr ((value lsr 8) land 0xFF));
      Bytes.set bytes (offset + 2) (Char.chr ((value lsr 16) land 0xFF));
      Bytes.set bytes (offset + 3) (Char.chr ((value lsr 24) land 0xFF));
    in

    set_le32 0 d.months;
    set_le32 4 d.days;
    set_le32 8 d.millis;
    bytes
  in

  let decode_duration bytes =
    if Bytes.length bytes <> 12 then
      invalid_arg "Duration must be exactly 12 bytes";

    let get_le32 offset =
      let b0 = Char.code (Bytes.get bytes offset) in
      let b1 = Char.code (Bytes.get bytes (offset + 1)) in
      let b2 = Char.code (Bytes.get bytes (offset + 2)) in
      let b3 = Char.code (Bytes.get bytes (offset + 3)) in
      b0 lor (b1 lsl 8) lor (b2 lsl 16) lor (b3 lsl 24)
    in

    {
      months = get_le32 0;
      days = get_le32 4;
      millis = get_le32 8;
    }
  in

  {
    Codec.schema = schema;
    encode = (fun d out ->
      let bytes = encode_duration d in
      Output.write_fixed out bytes);
    decode = (fun inp ->
      let bytes = Input.read_fixed inp 12 in
      decode_duration bytes);
  }

let make_logical ~name:_ ~base_codec:(base_codec : _ Codec.t) ~encode ~decode : _ Codec.t = {
  schema = base_codec.schema;
  encode = (fun value out ->
    let base_value = encode value in
    base_codec.encode base_value out);
  decode = (fun inp ->
    let base_value = base_codec.decode inp in
    decode base_value);
}
