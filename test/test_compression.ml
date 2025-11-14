(** Tests for compression codecs *)

open Avro

(* Force Avro module to initialize *)
let () = ignore (Avro.init_codecs)

(* ========== CODEC REGISTRY TESTS ========== *)

let test_registry_list () =
  let codecs = Codec_registry.list () in
  (* At minimum, null and deflate should always be available *)
  Alcotest.(check bool) "null codec available"
    true (List.mem "null" codecs);
  Alcotest.(check bool) "deflate codec available"
    true (List.mem "deflate" codecs)

let test_registry_get_null () =
  match Codec_registry.get "null" with
  | Some (module C : Codec_registry.CODEC) ->
      Alcotest.(check string) "codec name" "null" C.name
  | None ->
      Alcotest.fail "null codec should be available"

let test_registry_get_deflate () =
  match Codec_registry.get "deflate" with
  | Some (module C : Codec_registry.CODEC) ->
      Alcotest.(check string) "codec name" "deflate" C.name
  | None ->
      Alcotest.fail "deflate codec should be available"

let test_registry_get_unknown () =
  match Codec_registry.get "unknown-codec" with
  | Some _ ->
      Alcotest.fail "unknown codec should not be available"
  | None -> ()

(* ========== NULL CODEC TESTS ========== *)

let test_null_roundtrip () =
  match Codec_registry.get "null" with
  | Some (module C : Codec_registry.CODEC) ->
      let codec = C.create () in
      let data = Bytes.of_string "Hello, World!" in
      let compressed = C.compress codec data in
      let decompressed = C.decompress codec compressed in
      Alcotest.(check bytes) "null codec is passthrough"
        data decompressed
  | None ->
      Alcotest.fail "null codec should be available"

(* ========== DEFLATE CODEC TESTS ========== *)

let test_deflate_roundtrip () =
  match Codec_registry.get "deflate" with
  | Some (module C : Codec_registry.CODEC) ->
      let codec = C.create () in
      let data = Bytes.of_string "Hello, World! This is a test string for deflate compression." in
      let compressed = C.compress codec data in
      let decompressed = C.decompress codec compressed in
      Alcotest.(check bytes) "deflate roundtrip preserves data"
        data decompressed
  | None ->
      Alcotest.fail "deflate codec should be available"

let test_deflate_compresses () =
  match Codec_registry.get "deflate" with
  | Some (module C : Codec_registry.CODEC) ->
      let codec = C.create () in
      (* Use repetitive data that compresses well *)
      let data = Bytes.of_string (String.make 1000 'A') in
      let compressed = C.compress codec data in
      (* Compressed size should be significantly smaller *)
      Alcotest.(check bool) "deflate reduces size"
        true (Bytes.length compressed < Bytes.length data)
  | None ->
      Alcotest.fail "deflate codec should be available"

(* ========== OPTIONAL CODEC TESTS ========== *)

let test_zstd_roundtrip () =
  match Codec_registry.get "zstandard" with
  | Some (module C : Codec_registry.CODEC) ->
      let codec = C.create () in
      let data = Bytes.of_string "Hello, Zstandard! This is a test string." in
      let compressed = C.compress codec data in
      let decompressed = C.decompress codec compressed in
      Alcotest.(check bytes) "zstandard roundtrip preserves data"
        data decompressed
  | None ->
      (* Skip test if zstd not installed *)
      Alcotest.skip ()

let test_snappy_roundtrip () =
  match Codec_registry.get "snappy" with
  | Some (module C : Codec_registry.CODEC) ->
      let codec = C.create () in
      let data = Bytes.of_string "Hello, Snappy! This is a test string." in
      let compressed = C.compress codec data in
      let decompressed = C.decompress codec compressed in
      Alcotest.(check bytes) "snappy roundtrip preserves data"
        data decompressed
  | None ->
      (* Skip test if snappy not installed *)
      Alcotest.skip ()

(* ========== CUSTOM CODEC REGISTRATION ========== *)

let test_custom_codec_registration () =
  (* Register a simple ROT13 "compression" codec for testing *)
  let module ROT13_Codec = struct
    type t = unit
    let name = "rot13-test"
    let create () = ()
    let compress () data =
      Bytes.map (fun c ->
        if c >= 'a' && c <= 'z' then
          char_of_int ((int_of_char c - int_of_char 'a' + 13) mod 26 + int_of_char 'a')
        else if c >= 'A' && c <= 'Z' then
          char_of_int ((int_of_char c - int_of_char 'A' + 13) mod 26 + int_of_char 'A')
        else
          c
      ) data
    let decompress = compress  (* ROT13 is its own inverse *)
  end in

  Codec_registry.register "rot13-test" (module ROT13_Codec);

  (* Verify it was registered *)
  match Codec_registry.get "rot13-test" with
  | Some (module C : Codec_registry.CODEC) ->
      Alcotest.(check string) "custom codec name" "rot13-test" C.name;

      (* Test roundtrip *)
      let codec = C.create () in
      let data = Bytes.of_string "Hello" in
      let compressed = C.compress codec data in
      let decompressed = C.decompress codec compressed in
      Alcotest.(check bytes) "custom codec roundtrip"
        data decompressed
  | None ->
      Alcotest.fail "custom codec should be registered"

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Compression" [
    "registry", [
      test_case "list codecs" `Quick test_registry_list;
      test_case "get null codec" `Quick test_registry_get_null;
      test_case "get deflate codec" `Quick test_registry_get_deflate;
      test_case "get unknown codec" `Quick test_registry_get_unknown;
    ];

    "null codec", [
      test_case "roundtrip" `Quick test_null_roundtrip;
    ];

    "deflate codec", [
      test_case "roundtrip" `Quick test_deflate_roundtrip;
      test_case "compresses data" `Quick test_deflate_compresses;
    ];

    "optional codecs", [
      test_case "zstandard roundtrip" `Quick test_zstd_roundtrip;
      test_case "snappy roundtrip" `Quick test_snappy_roundtrip;
    ];

    "custom codecs", [
      test_case "registration" `Quick test_custom_codec_registration;
    ];
  ]
