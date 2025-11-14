(** Basic usage example demonstrating primitive types and arrays *)

let () =
  Printf.printf "=== Avro Basic Usage Example ===\n\n";

  (* Example 1: Encoding and decoding a string *)
  let name = "Alice" in
  Printf.printf "1. String encoding/decoding:\n";
  Printf.printf "   Original: %s\n" name;

  let encoded = Avro.Codec.encode_to_bytes Avro.Codec.string name in
  Printf.printf "   Encoded size: %d bytes\n" (Bytes.length encoded);

  let decoded = Avro.Codec.decode_from_bytes Avro.Codec.string encoded in
  Printf.printf "   Decoded: %s\n\n" decoded;

  (* Example 2: Encoding and decoding an integer *)
  let age = 42 in
  Printf.printf "2. Integer encoding/decoding:\n";
  Printf.printf "   Original: %d\n" age;

  let encoded = Avro.Codec.encode_to_bytes Avro.Codec.int age in
  Printf.printf "   Encoded size: %d bytes\n" (Bytes.length encoded);

  let decoded = Avro.Codec.decode_from_bytes Avro.Codec.int encoded in
  Printf.printf "   Decoded: %d\n\n" decoded;

  (* Example 3: Encoding and decoding an array *)
  let numbers = [| 1; 2; 3; 4; 5 |] in
  Printf.printf "3. Array encoding/decoding:\n";
  Printf.printf "   Original: [|%s|]\n"
    (String.concat "; " (Array.to_list (Array.map string_of_int numbers)));

  let codec = Avro.Codec.array Avro.Codec.int in
  let encoded = Avro.Codec.encode_to_bytes codec numbers in
  Printf.printf "   Encoded size: %d bytes\n" (Bytes.length encoded);

  let decoded = Avro.Codec.decode_from_bytes codec encoded in
  Printf.printf "   Decoded: [|%s|]\n\n"
    (String.concat "; " (Array.to_list (Array.map string_of_int decoded)));

  (* Example 4: Optional values *)
  Printf.printf "4. Optional value encoding/decoding:\n";

  let some_value = Some "hello" in
  let codec = Avro.Codec.option Avro.Codec.string in
  let encoded = Avro.Codec.encode_to_bytes codec some_value in
  let decoded = Avro.Codec.decode_from_bytes codec encoded in
  Printf.printf "   Some \"hello\" -> %s\n"
    (match decoded with Some s -> "Some \"" ^ s ^ "\"" | None -> "None");

  let none_value = None in
  let encoded = Avro.Codec.encode_to_bytes codec none_value in
  let decoded = Avro.Codec.decode_from_bytes codec encoded in
  Printf.printf "   None -> %s\n"
    (match decoded with Some s -> "Some \"" ^ s ^ "\"" | None -> "None");

  Printf.printf "\n=== Done ===\n"
