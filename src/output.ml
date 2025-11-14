type t = {
  buffer: Buffer.t;
}

(* TODO This could be tuned more, supply buffer size as optional argument. *)
let create () = { buffer = Buffer.create 1024 }

let of_buffer buffer = { buffer }

let contents t = Buffer.contents t.buffer

let to_bytes t = Bytes.of_string (contents t)

let zigzag32 n =
  let n32 = Int32.of_int n in
  Int32.(logxor (shift_left n32 1) (shift_right n32 31))

let zigzag64 n =
  Int64.(logxor (shift_left n 1) (shift_right n 63))

let write_long t n =
  let rec loop n =
    let byte = Int64.(to_int (logand n 0x7FL)) in
    let n' = Int64.shift_right_logical n 7 in
    if n' = 0L then
      Buffer.add_char t.buffer (Char.chr byte)
    else begin
      Buffer.add_char t.buffer (Char.chr (byte lor 0x80));
      loop n'
    end
  in
  loop (zigzag64 n)

let write_int t n =
  write_long t (Int64.of_int n)

let write_null _t () = ()

let write_boolean t b =
  Buffer.add_char t.buffer (if b then '\x01' else '\x00')

let write_float t f =
  let bits = Int32.bits_of_float f in
  for i = 0 to 3 do
    let byte = Int32.(to_int (shift_right_logical bits (i * 8)) land 0xff) in
    Buffer.add_char t.buffer (Char.chr byte)
  done

let write_double t f =
  let bits = Int64.bits_of_float f in
  for i = 0 to 7 do
    let byte = Int64.(to_int (shift_right_logical bits (i * 8)) land 0xff) in
    Buffer.add_char t.buffer (Char.chr byte)
  done

let write_bytes t bytes =
  write_long t (Int64.of_int (Bytes.length bytes));
  Buffer.add_bytes t.buffer bytes

let write_string t str =
  write_long t (Int64.of_int (String.length str));
  Buffer.add_string t.buffer str

let write_fixed t bytes =
  Buffer.add_bytes t.buffer bytes
