(* Use bytes as backing storage for better semantics with binary data.
   Future: Add bigstring/mmap support for very large files. *)
type t = {
  data: bytes;
  mutable pos: int;
}

let of_bytes data = { data; pos = 0 }

let of_string str = { data = Bytes.of_string str; pos = 0 }

let position t = t.pos

let remaining t = Bytes.length t.data - t.pos

let at_end t = t.pos >= Bytes.length t.data

exception End_of_input

let unzigzag32 n =
  let result = Int32.(logxor (shift_right_logical n 1) (neg (logand n 1l))) in
  Int32.to_int result

let unzigzag64 n =
  Int64.(logxor (shift_right_logical n 1) (neg (logand n 1L)))

let read_long t =
  let rec loop acc shift =
    if t.pos >= Bytes.length t.data then raise End_of_input;
    let byte = Char.code (Bytes.get t.data t.pos) in
    t.pos <- t.pos + 1;
    let acc' = Int64.(logor acc (shift_left (of_int (byte land 0x7f)) shift)) in
    if byte land 0x80 = 0 then
      unzigzag64 acc'
    else
      (loop[@tailcall]) acc' (shift + 7)
  in
  loop 0L 0

let read_int t =
  Int64.to_int (read_long t)

let read_null _t = ()

let read_boolean t =
  if t.pos >= Bytes.length t.data then raise End_of_input;
  let byte = Char.code (Bytes.get t.data t.pos) in
  t.pos <- t.pos + 1;
  byte <> 0

let read_float t =
  if t.pos + 4 > Bytes.length t.data then raise End_of_input;
  let bits = ref 0l in
  for i = 0 to 3 do
    let byte = Int32.of_int (Char.code (Bytes.get t.data (t.pos + i))) in
    bits := Int32.(logor !bits (shift_left byte (i * 8)))
  done;
  t.pos <- t.pos + 4;
  Int32.float_of_bits !bits

let read_double t =
  if t.pos + 8 > Bytes.length t.data then raise End_of_input;
  let bits = ref 0L in
  for i = 0 to 7 do
    let byte = Int64.of_int (Char.code (Bytes.get t.data (t.pos + i))) in
    bits := Int64.(logor !bits (shift_left byte (i * 8)))
  done;
  t.pos <- t.pos + 8;
  Int64.float_of_bits !bits

let read_bytes t =
  let len = read_int t in
  if t.pos + len > Bytes.length t.data then raise End_of_input;
  let result = Bytes.sub t.data t.pos len in
  t.pos <- t.pos + len;
  result

let read_string t =
  let len = read_int t in
  if t.pos + len > Bytes.length t.data then raise End_of_input;
  let result = Bytes.sub_string t.data t.pos len in
  t.pos <- t.pos + len;
  result

let read_fixed t size =
  if t.pos + size > Bytes.length t.data then raise End_of_input;
  let result = Bytes.sub t.data t.pos size in
  t.pos <- t.pos + size;
  result
