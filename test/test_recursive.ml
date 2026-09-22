(** Tests for recursive types like linked lists and trees *)

open Avro_simple

(* Ensure codecs are registered *)
let () = Avro.init_codecs ()

(** Linked list encoded as recursive record with optional next pointer *)
type linked_node = {
  value: int;
  next: linked_node option;
}

(** Tree encoded as recursive record with optional left/right children *)
type tree_node = {
  tree_value: int;
  left: tree_node option;
  right: tree_node option;
}

(** Build a recursive linked list codec using the new recursive combinator *)
let linked_list_codec () =
  Codec.option (
    Codec.recursive (fun self ->
      Codec.record (Type_name.simple "LinkedNode") (fun value next -> { value; next })
      |> Codec.field "value" Codec.int (fun r -> r.value)
      |> Codec.field "next" (Codec.option self) (fun r -> r.next)
      |> Codec.finish
    )
  )

(** Build a recursive linked list codec using manual fixpoint (old approach) *)
let linked_list_codec_manual () =
  (* Forward references for the option codec *)
  let option_schema_ref = ref (Schema.Union [Schema.Null; Schema.Null]) in
  let option_encode_ref = ref (fun _ _ -> ()) in
  let option_decode_ref = ref (fun _ -> None) in

  (* Create the recursive node codec *)
  let node_codec =
    Codec.record (Type_name.simple "LinkedNode") (fun value next -> { value; next })
    |> Codec.field "value" Codec.int (fun r -> r.value)
    |> Codec.field "next" {
        Codec.schema = !option_schema_ref;
        Codec.encode = (fun v out -> !option_encode_ref v out);
        Codec.decode = (fun inp -> !option_decode_ref inp);
      } (fun r -> r.next)
    |> Codec.finish
  in

  (* Create the full optional codec *)
  let full_codec = Codec.option node_codec in

  (* Fill in the references with the option codec *)
  option_schema_ref := full_codec.Codec.schema;
  option_encode_ref := full_codec.Codec.encode;
  option_decode_ref := full_codec.Codec.decode;

  full_codec

(** Build a recursive tree codec using the new recursive combinator *)
let tree_codec () =
  Codec.option (
    Codec.recursive (fun self ->
      Codec.record (Type_name.simple "TreeNode")
        (fun tree_value left right -> { tree_value; left; right })
      |> Codec.field "value" Codec.int (fun r -> r.tree_value)
      |> Codec.field "left" (Codec.option self) (fun r -> r.left)
      |> Codec.field "right" (Codec.option self) (fun r -> r.right)
      |> Codec.finish
    )
  )

(** Build a recursive tree codec using manual fixpoint (old approach) *)
let tree_codec_manual () =
  (* Forward references for the option codec *)
  let option_schema_ref = ref (Schema.Union [Schema.Null; Schema.Null]) in
  let option_encode_ref = ref (fun _ _ -> ()) in
  let option_decode_ref = ref (fun _ -> None) in

  (* Create the recursive node codec *)
  let node_codec =
    Codec.record (Type_name.simple "TreeNode")
      (fun tree_value left right -> { tree_value; left; right })
    |> Codec.field "value" Codec.int (fun r -> r.tree_value)
    |> Codec.field "left" {
        Codec.schema = !option_schema_ref;
        Codec.encode = (fun v out -> !option_encode_ref v out);
        Codec.decode = (fun inp -> !option_decode_ref inp);
      } (fun r -> r.left)
    |> Codec.field "right" {
        Codec.schema = !option_schema_ref;
        Codec.encode = (fun v out -> !option_encode_ref v out);
        Codec.decode = (fun inp -> !option_decode_ref inp);
      } (fun r -> r.right)
    |> Codec.finish
  in

  (* Create the full optional codec *)
  let full_codec = Codec.option node_codec in

  (* Fill in the references with the option codec *)
  option_schema_ref := full_codec.Codec.schema;
  option_encode_ref := full_codec.Codec.encode;
  option_decode_ref := full_codec.Codec.decode;

  full_codec

(** Test encoding and decoding a simple linked list *)
let test_linked_list_empty () =
  let codec = linked_list_codec () in

  let node = None in
  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  Alcotest.(check bool) "empty list" true (decoded = None)

let test_linked_list_single () =
  let codec = linked_list_codec () in

  let node = Some { value = 42; next = None } in
  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  match decoded with
  | Some { value; next = None } -> Alcotest.(check int) "value" 42 value
  | _ -> Alcotest.fail "Expected single node"

let test_linked_list_multiple () =
  let codec = linked_list_codec () in

  (* Create list: 1 -> 2 -> 3 -> None *)
  let node = Some {
    value = 1;
    next = Some {
      value = 2;
      next = Some {
        value = 3;
        next = None
      }
    }
  } in

  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  match decoded with
  | Some { value = v1; next = Some { value = v2; next = Some { value = v3; next = None }}} ->
      Alcotest.(check int) "first" 1 v1;
      Alcotest.(check int) "second" 2 v2;
      Alcotest.(check int) "third" 3 v3
  | _ -> Alcotest.fail "Expected three nodes"

let test_linked_list_roundtrip () =
  let codec = linked_list_codec () in

  (* Test with various list lengths *)
  let test_length n =
    let rec make_list i =
      if i > n then None
      else Some { value = i; next = make_list (i + 1) }
    in

    let original = make_list 1 in
    let out = Output.create () in
    codec.Codec.encode original out;

    let bytes = Output.to_bytes out in
    let inp = Input.of_bytes bytes in
    let decoded = codec.Codec.decode inp in

    (* Compare by converting to lists *)
    let rec to_list = function
      | None -> []
      | Some { value; next } -> value :: to_list next
    in

    Alcotest.(check (list int))
      (Printf.sprintf "roundtrip length %d" n)
      (to_list original)
      (to_list decoded)
  in

  test_length 0;
  test_length 1;
  test_length 5;
  test_length 10

(** Test encoding and decoding a binary tree *)
let test_tree_empty () =
  let codec = tree_codec () in

  let node = None in
  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  Alcotest.(check bool) "empty tree" true (decoded = None)

let test_tree_single () =
  let codec = tree_codec () in

  let node = Some { tree_value = 42; left = None; right = None } in
  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  match decoded with
  | Some { tree_value; left = None; right = None } ->
      Alcotest.(check int) "value" 42 tree_value
  | _ -> Alcotest.fail "Expected single node"

let test_tree_balanced () =
  let codec = tree_codec () in

  (* Create balanced tree:
         1
        / \
       2   3
  *)
  let node = Some {
    tree_value = 1;
    left = Some { tree_value = 2; left = None; right = None };
    right = Some { tree_value = 3; left = None; right = None };
  } in

  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  match decoded with
  | Some { tree_value = v1;
           left = Some { tree_value = v2; left = None; right = None };
           right = Some { tree_value = v3; left = None; right = None } } ->
      Alcotest.(check int) "root" 1 v1;
      Alcotest.(check int) "left" 2 v2;
      Alcotest.(check int) "right" 3 v3
  | _ -> Alcotest.fail "Expected balanced tree"

let test_tree_deep () =
  let codec = tree_codec () in

  (* Create deep tree (right spine):
       1
        \
         2
          \
           3
  *)
  let node = Some {
    tree_value = 1;
    left = None;
    right = Some {
      tree_value = 2;
      left = None;
      right = Some {
        tree_value = 3;
        left = None;
        right = None;
      }
    }
  } in

  let out = Output.create () in
  codec.Codec.encode node out;

  let bytes = Output.to_bytes out in
  let inp = Input.of_bytes bytes in
  let decoded = codec.Codec.decode inp in

  (* Verify by extracting values along right spine *)
  let rec right_spine = function
    | None -> []
    | Some { tree_value; right; _ } -> tree_value :: right_spine right
  in

  Alcotest.(check (list int)) "deep tree values"
    [1; 2; 3]
    (right_spine decoded)

(* Note: Schema generation tests for recursive types are complex because
   recursive schemas in Avro should use name references to avoid infinite inline expansion.
   Our current fixpoint approach works correctly for encoding/decoding but the schema
   representation needs improvement to properly reference types by name. *)

(** Test that manual fixpoint codec produces same results as recursive combinator *)
let test_linked_list_manual_vs_combinator () =
  let codec_combinator = linked_list_codec () in
  let codec_manual = linked_list_codec_manual () in

  (* Test with various list lengths *)
  let test_length n =
    let rec make_list i =
      if i > n then None
      else Some { value = i; next = make_list (i + 1) }
    in

    let original = make_list 1 in

    (* Encode with combinator *)
    let out1 = Output.create () in
    codec_combinator.Codec.encode original out1;
    let bytes1 = Output.to_bytes out1 in

    (* Encode with manual *)
    let out2 = Output.create () in
    codec_manual.Codec.encode original out2;
    let bytes2 = Output.to_bytes out2 in

    (* Both should produce identical bytes *)
    Alcotest.(check bytes)
      (Printf.sprintf "identical encoding for length %d" n)
      bytes1 bytes2;

    (* Decode from combinator codec should work with manual codec bytes *)
    let inp = Input.of_bytes bytes2 in
    let decoded = codec_combinator.Codec.decode inp in

    let rec to_list = function
      | None -> []
      | Some { value; next } -> value :: to_list next
    in

    Alcotest.(check (list int))
      (Printf.sprintf "cross-decode length %d" n)
      (to_list original)
      (to_list decoded)
  in

  test_length 0;
  test_length 1;
  test_length 5;
  test_length 10

(** Test that manual fixpoint tree codec produces same results as recursive combinator *)
let test_tree_manual_vs_combinator () =
  let codec_combinator = tree_codec () in
  let codec_manual = tree_codec_manual () in

  (* Create balanced tree:
         1
        / \
       2   3
  *)
  let tree = Some {
    tree_value = 1;
    left = Some { tree_value = 2; left = None; right = None };
    right = Some { tree_value = 3; left = None; right = None };
  } in

  (* Encode with combinator *)
  let out1 = Output.create () in
  codec_combinator.Codec.encode tree out1;
  let bytes1 = Output.to_bytes out1 in

  (* Encode with manual *)
  let out2 = Output.create () in
  codec_manual.Codec.encode tree out2;
  let bytes2 = Output.to_bytes out2 in

  (* Both should produce identical bytes *)
  Alcotest.(check bytes) "identical tree encoding" bytes1 bytes2;

  (* Decode from combinator codec should work with manual codec bytes *)
  let inp = Input.of_bytes bytes2 in
  let decoded = codec_combinator.Codec.decode inp in

  match decoded with
  | Some { tree_value = v1;
           left = Some { tree_value = v2; left = None; right = None };
           right = Some { tree_value = v3; left = None; right = None } } ->
      Alcotest.(check int) "root" 1 v1;
      Alcotest.(check int) "left" 2 v2;
      Alcotest.(check int) "right" 3 v3
  | _ -> Alcotest.fail "Expected balanced tree from cross-decode"

(** Test with container files *)
let test_linked_list_container () =
  let codec = linked_list_codec () in
  let test_file = Filename.concat (Filename.get_temp_dir_name ()) "test_recursive_list.avro" in

  (* Write linked list to container file *)
  let writer = Container_writer.create ~path:test_file ~codec () in
  Container_writer.write writer None;  (* Empty list *)
  Container_writer.write writer (Some { value = 1; next = None });  (* Single *)
  Container_writer.write writer (Some {
    value = 1;
    next = Some { value = 2; next = None }
  });  (* Two elements *)
  Container_writer.close writer;

  (* Read back *)
  let reader = Container_reader.open_file ~path:test_file ~codec () in
  let values = Container_reader.fold (fun acc v -> v :: acc) [] reader in
  Container_reader.close reader;

  Alcotest.(check int) "container count" 3 (List.length values);

  (* Verify values (reversed due to fold) *)
  let v1 = List.nth values 2 in
  let v2 = List.nth values 1 in
  let v3 = List.nth values 0 in

  Alcotest.(check bool) "first is empty" true (v1 = None);

  begin match v2 with
  | Some { value = 1; next = None } -> ()
  | _ -> Alcotest.fail "Expected single element"
  end;

  begin match v3 with
  | Some { value = 1; next = Some { value = 2; next = None }} -> ()
  | _ -> Alcotest.fail "Expected two elements"
  end

let () =
  let open Alcotest in
  run "Recursive Types" [
    "linked list", [
      test_case "empty list" `Quick test_linked_list_empty;
      test_case "single node" `Quick test_linked_list_single;
      test_case "multiple nodes" `Quick test_linked_list_multiple;
      test_case "roundtrip various lengths" `Quick test_linked_list_roundtrip;
      test_case "manual vs combinator" `Quick test_linked_list_manual_vs_combinator;
      test_case "container file" `Quick test_linked_list_container;
    ];
    "binary tree", [
      test_case "empty tree" `Quick test_tree_empty;
      test_case "single node" `Quick test_tree_single;
      test_case "balanced tree" `Quick test_tree_balanced;
      test_case "deep tree" `Quick test_tree_deep;
      test_case "manual vs combinator" `Quick test_tree_manual_vs_combinator;
    ];
  ]
