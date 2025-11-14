(** Schema compatibility example

    This example demonstrates different schema patterns and how to work
    with evolving data structures using codecs.
*)
open Avro_simple

(* Type definitions *)
type person_v1 = { name: string }
type person_v2 = { name: string; email: string option }
type product_v1 = { id: int }
type product_v2 = { id: int; description: string option }
type user_old = { username: string; id: int }
type user_new = { id: int; username: string }

let () =
  Printf.printf "=== Avro Schema Compatibility Example ===\n\n";

  (* Example 1: Adding an optional field *)
  Printf.printf "1. Adding an optional field:\n";

  let person_v1_codec =
    Avro.Codec.record (Avro.Type_name.simple "Person") (fun name -> ({ name } : person_v1))
    |> Avro.Codec.field "name" Avro.Codec.string (fun (p : person_v1) -> p.name)
    |> Avro.Codec.finish in

  (* Encode with version 1 *)
  let alice_v1 = { name = "Alice" } in
  let encoded_v1 = Avro.Codec.encode_to_bytes person_v1_codec alice_v1 in
  Printf.printf "   Encoded v1 (name only): %d bytes\n" (Bytes.length encoded_v1);

  (* Decode with v1 codec *)
  let decoded_v1 = Avro.Codec.decode_from_bytes person_v1_codec encoded_v1 in
  Printf.printf "   Decoded v1: name=%s\n" decoded_v1.name;

  (* Version 2 codec with optional email field *)
  let person_v2_codec =
    Avro.Codec.record (Avro.Type_name.simple "Person") (fun name email -> ({ name; email } : person_v2))
    |> Avro.Codec.field "name" Avro.Codec.string (fun (p : person_v2) -> p.name)
    |> Avro.Codec.field_opt "email" Avro.Codec.string (fun (p : person_v2) -> p.email)
    |> Avro.Codec.finish in

  (* Encode with v2 codec *)
  let bob_v2 = { name = "Bob"; email = Some "bob@example.com" } in
  let encoded_v2 = Avro.Codec.encode_to_bytes person_v2_codec bob_v2 in
  Printf.printf "   Encoded v2 (with email): %d bytes\n" (Bytes.length encoded_v2);

  (* Decode with v2 codec *)
  let decoded_v2 = Avro.Codec.decode_from_bytes person_v2_codec encoded_v2 in
  Printf.printf "   Decoded v2: name=%s, email=%s\n\n"
    decoded_v2.name
    (match decoded_v2.email with Some e -> e | None -> "(none)");

  (* Example 2: Working with long integers *)
  Printf.printf "2. Long integer encoding/decoding:\n";

  (* Writer uses long *)
  let age_long = 12345678901234L in
  let encoded_long = Avro.Codec.encode_to_bytes Avro.Codec.long age_long in
  Printf.printf "   Encoded as long: %Ld\n" age_long;

  (* Reader decodes as long *)
  let decoded_long = Avro.Codec.decode_from_bytes Avro.Codec.long encoded_long in
  Printf.printf "   Decoded as long: %Ld\n\n" decoded_long;

  (* Example 3: Adding fields with defaults *)
  Printf.printf "3. Adding fields with defaults:\n";

  let product_v1_codec =
    Avro.Codec.record (Avro.Type_name.simple "Product") (fun id -> ({ id } : product_v1))
    |> Avro.Codec.field "id" Avro.Codec.int (fun (p : product_v1) -> p.id)
    |> Avro.Codec.finish in

  let product_old = { id = 123 } in
  let encoded_product = Avro.Codec.encode_to_bytes product_v1_codec product_old in
  Printf.printf "   Encoded old product (id only): %d bytes\n" (Bytes.length encoded_product);

  let decoded_product_v1 = Avro.Codec.decode_from_bytes product_v1_codec encoded_product in
  Printf.printf "   Decoded with v1 schema: id=%d\n" decoded_product_v1.id;

  let product_v2_codec =
    Avro.Codec.record (Avro.Type_name.simple "Product") (fun id description -> ({ id; description } : product_v2))
    |> Avro.Codec.field "id" Avro.Codec.int (fun (p : product_v2) -> p.id)
    |> Avro.Codec.field_opt "description" Avro.Codec.string (fun (p : product_v2) -> p.description)
    |> Avro.Codec.finish in

  let product_new = { id = 456; description = Some "Premium Widget" } in
  let encoded_product_v2 = Avro.Codec.encode_to_bytes product_v2_codec product_new in
  Printf.printf "   Encoded new product (with description): %d bytes\n" (Bytes.length encoded_product_v2);

  let decoded_product_v2 = Avro.Codec.decode_from_bytes product_v2_codec encoded_product_v2 in
  Printf.printf "   Decoded with v2 schema: id=%d, description=%s\n\n"
    decoded_product_v2.id
    (match decoded_product_v2.description with Some d -> d | None -> "(none)");

  (* Example 4: Field reordering *)
  Printf.printf "4. Field reordering:\n";

  let user_old_codec =
    Avro.Codec.record (Avro.Type_name.simple "User") (fun username id -> ({ username; id } : user_old))
    |> Avro.Codec.field "username" Avro.Codec.string (fun (u : user_old) -> u.username)
    |> Avro.Codec.field "id" Avro.Codec.int (fun (u : user_old) -> u.id)
    |> Avro.Codec.finish in

  let user : user_old = { username = "charlie"; id = 42 } in
  let encoded_user = Avro.Codec.encode_to_bytes user_old_codec user in
  Printf.printf "   Encoded user with field order: username, id\n";

  let decoded_user_old = Avro.Codec.decode_from_bytes user_old_codec encoded_user in
  Printf.printf "   Decoded: username=%s, id=%d\n" decoded_user_old.username decoded_user_old.id;

  (* New schema with fields in different order *)
  let user_new_codec =
    Avro.Codec.record (Avro.Type_name.simple "User") (fun id username -> ({ id; username } : user_new))
    |> Avro.Codec.field "id" Avro.Codec.int (fun (u : user_new) -> u.id)
    |> Avro.Codec.field "username" Avro.Codec.string (fun (u : user_new) -> u.username)
    |> Avro.Codec.finish in

  let user_new : user_new = { id = 99; username = "diana" } in
  let encoded_user_new = Avro.Codec.encode_to_bytes user_new_codec user_new in
  Printf.printf "   Encoded user with field order: id, username\n";

  let decoded_user_new = Avro.Codec.decode_from_bytes user_new_codec encoded_user_new in
  Printf.printf "   Decoded: id=%d, username=%s\n\n" decoded_user_new.id decoded_user_new.username;

  (* Example 5: Union with same-type branches *)
  Printf.printf "5. Union with multiple branches:\n";

  (* Writer and reader both use union of int options *)
  let int_union_codec = Avro.Codec.union [Avro.Codec.int; Avro.Codec.int; Avro.Codec.int] in
  let value_branch_1 = (1, 42) in
  let encoded_union = Avro.Codec.encode_to_bytes int_union_codec value_branch_1 in
  Printf.printf "   Encoded union: branch=%d, value=%d\n" (fst value_branch_1) (snd value_branch_1);

  let (branch, value) = Avro.Codec.decode_from_bytes int_union_codec encoded_union in
  Printf.printf "   Decoded union: branch=%d, value=%d\n\n" branch value;

  (* Example 6: Arrays of different types *)
  Printf.printf "6. Arrays of different types:\n";

  (* Array of ints *)
  let int_array = [| 10; 20; 30 |] in
  let int_array_codec = Avro.Codec.array Avro.Codec.int in
  let encoded_int_array = Avro.Codec.encode_to_bytes int_array_codec int_array in
  Printf.printf "   Encoded int array: [10, 20, 30]\n";

  let decoded_int_array = Avro.Codec.decode_from_bytes int_array_codec encoded_int_array in
  Printf.printf "   Decoded int array: [%s]\n"
    (String.concat ", " (Array.to_list (Array.map string_of_int decoded_int_array)));

  (* Array of longs *)
  let long_array = [| 100L; 200L; 300L |] in
  let long_array_codec = Avro.Codec.array Avro.Codec.long in
  let encoded_long_array = Avro.Codec.encode_to_bytes long_array_codec long_array in
  Printf.printf "   Encoded long array: [100, 200, 300]\n";

  let decoded_long_array = Avro.Codec.decode_from_bytes long_array_codec encoded_long_array in
  Printf.printf "   Decoded long array: [%s]\n"
    (String.concat ", " (Array.to_list (Array.map Int64.to_string decoded_long_array)));

  Printf.printf "\n=== Schema Compatibility Key Takeaways ===\n";
  Printf.printf "✓ Optional fields enable backward compatibility\n";
  Printf.printf "✓ Different schema versions can coexist\n";
  Printf.printf "✓ Field order doesn't matter - matched by name\n";
  Printf.printf "✓ Union types support multiple branches\n";
  Printf.printf "✓ Arrays support different element types\n";
  Printf.printf "✓ For true schema evolution, use Container files (see container_files.ml)\n";
  Printf.printf "\n=== Done ===\n"
