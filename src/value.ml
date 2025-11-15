type t =
  | Null
  | Boolean of bool
  | Int of int
  | Long of int64
  | Float of float
  | Double of float
  | Bytes of bytes
  | String of string
  | Array of t array
  | Map of (string * t) list
  | Record of (string * t) list
  | Enum of int * string
  | Union of int * t
  | Fixed of bytes

let rec equal v1 v2 =
  match v1, v2 with
  | Null, Null -> true
  | Boolean b1, Boolean b2 -> b1 = b2
  | Int i1, Int i2 -> i1 = i2
  | Long l1, Long l2 -> l1 = l2
  | Float f1, Float f2 -> f1 = f2
  | Double d1, Double d2 -> d1 = d2
  | Bytes b1, Bytes b2 -> Bytes.equal b1 b2
  | String s1, String s2 -> s1 = s2
  | Array a1, Array a2 ->
      Array.length a1 = Array.length a2 &&
      Array.for_all2 equal a1 a2
  | Map m1, Map m2 ->
      List.length m1 = List.length m2 &&
      List.for_all2 (fun (k1, v1) (k2, v2) -> k1 = k2 && equal v1 v2) m1 m2
  | Record r1, Record r2 ->
      List.length r1 = List.length r2 &&
      List.for_all2 (fun (k1, v1) (k2, v2) -> k1 = k2 && equal v1 v2) r1 r2
  | Enum (i1, s1), Enum (i2, s2) -> i1 = i2 && s1 = s2
  | Union (i1, v1), Union (i2, v2) -> i1 = i2 && equal v1 v2
  | Fixed b1, Fixed b2 -> Bytes.equal b1 b2
  | _ -> false

let rec of_default = function
  | Schema.Null_default -> Null
  | Schema.Bool_default b -> Boolean b
  | Schema.Int_default i -> Int i
  | Schema.Long_default l -> Long l
  | Schema.Float_default f -> Float f
  | Schema.Double_default d -> Double d
  | Schema.Bytes_default b -> Bytes b
  | Schema.String_default s -> String s
  | Schema.Enum_default s ->
      Enum (0, s)
  | Schema.Array_default items ->
      let arr = Array.init (List.length items) (fun i -> of_default (List.nth items i)) in
      Array arr
  | Schema.Map_default pairs -> Map (List.map (fun (k, v) -> (k, of_default v)) pairs)
  | Schema.Union_default (branch, value) -> Union (branch, of_default value)

(* TODO This could be tidier with Format and pretty print functions *)
let rec to_string = function
  | Null -> "null"
  | Boolean b -> string_of_bool b
  | Int i -> string_of_int i
  | Long l -> Int64.to_string l ^ "L"
  | Float f -> string_of_float f ^ "f"
  | Double d -> string_of_float d
  | Bytes b -> Printf.sprintf "<%d bytes>" (Bytes.length b)
  | String s -> Printf.sprintf "\"%s\"" s
  | Array arr ->
      let items = Array.to_list arr |> List.map to_string |> String.concat ", " in
      Printf.sprintf "[%s]" items
  | Map pairs ->
      let items = List.map (fun (k, v) -> Printf.sprintf "\"%s\": %s" k (to_string v)) pairs
                  |> String.concat ", " in
      Printf.sprintf "{%s}" items
  | Record fields ->
      let items = List.map (fun (k, v) -> Printf.sprintf "\"%s\": %s" k (to_string v)) fields
                  |> String.concat ", " in
      Printf.sprintf "{%s}" items
  | Enum (i, s) -> Printf.sprintf "\"%s\"(%d)" s i
  | Union (i, v) -> Printf.sprintf "union<%d>(%s)" i (to_string v)
  | Fixed b -> Printf.sprintf "<fixed %d bytes>" (Bytes.length b)
