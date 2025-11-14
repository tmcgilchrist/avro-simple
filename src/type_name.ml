type t = {
  name: string;
  namespace: string list;
}

let make name namespace = { name; namespace }

let simple name = { name; namespace = [] }

let parse full_name =
  match String.split_on_char '.' full_name with
  | [] -> { name = ""; namespace = [] }
  | [n] -> { name = n; namespace = [] }
  | parts ->
      let rev_parts = List.rev parts in
      { name = List.hd rev_parts; namespace = List.rev (List.tl rev_parts) }

let full_name t =
  match t.namespace with
  | [] -> t.name
  | ns -> String.concat "." ns ^ "." ^ t.name

let base_name t = t.name

let namespace t =
  match t.namespace with
  | [] -> None
  | ns -> Some (String.concat "." ns)

let equal t1 t2 =
  t1.name = t2.name && t1.namespace = t2.namespace

let compare t1 t2 =
  match String.compare (full_name t1) (full_name t2) with
  | 0 -> 0
  | n -> n

let compatible t1 t2 =
  equal t1 t2

let compatible_names ~reader_name ~reader_aliases ~writer_name =
  reader_name.name = writer_name.name ||
  List.exists (fun alias ->
    let alias_tn = parse alias in
    equal alias_tn writer_name
  ) reader_aliases
