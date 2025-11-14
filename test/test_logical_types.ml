(** Tests for logical type conversion helpers *)

open Avro_simple
open Avro_simple.Logical

(* Force codec initialization *)
let () = ignore (Avro.init_codecs ())

(* ========== DATE TESTS ========== *)

let test_date_epoch () =
  (* Unix epoch: 1970-01-01 *)
  let value = (1970, 1, 1) in
  let encoded = Codec.encode_to_bytes date value in
  let decoded = Codec.decode_from_bytes date encoded in
  Alcotest.(check (triple int int int)) "epoch date roundtrip"
    value decoded

let test_date_recent () =
  (* 2024-11-13 *)
  let value = (2024, 11, 13) in
  let encoded = Codec.encode_to_bytes date value in
  let decoded = Codec.decode_from_bytes date encoded in
  Alcotest.(check (triple int int int)) "recent date roundtrip"
    value decoded

let test_date_pre_epoch () =
  (* 1969-12-31 (one day before epoch) *)
  let value = (1969, 12, 31) in
  let encoded = Codec.encode_to_bytes date value in
  let decoded = Codec.decode_from_bytes date encoded in
  Alcotest.(check (triple int int int)) "pre-epoch date roundtrip"
    value decoded

let test_date_leap_year () =
  (* 2024-02-29 (leap year) *)
  let value = (2024, 2, 29) in
  let encoded = Codec.encode_to_bytes date value in
  let decoded = Codec.decode_from_bytes date encoded in
  Alcotest.(check (triple int int int)) "leap year date roundtrip"
    value decoded

(* ========== TIME TESTS ========== *)

let time_testable = Alcotest.testable
  (fun ppf (h, m, s, ms) -> Format.fprintf ppf "(%d, %d, %d, %d)" h m s ms)
  (fun (h1, m1, s1, ms1) (h2, m2, s2, ms2) ->
    h1 = h2 && m1 = m2 && s1 = s2 && ms1 = ms2)

let test_time_millis_midnight () =
  let time = (0, 0, 0, 0) in
  let encoded = Codec.encode_to_bytes time_millis time in
  let decoded = Codec.decode_from_bytes time_millis encoded in
  Alcotest.(check time_testable) "midnight time-millis roundtrip"
    time decoded

let test_time_millis_noon () =
  let time = (12, 30, 45, 123) in
  let encoded = Codec.encode_to_bytes time_millis time in
  let decoded = Codec.decode_from_bytes time_millis encoded in
  Alcotest.(check time_testable) "noon time-millis roundtrip"
    time decoded

let test_time_micros_midnight () =
  let time = (0, 0, 0, 0) in
  let encoded = Codec.encode_to_bytes time_micros time in
  let decoded = Codec.decode_from_bytes time_micros encoded in
  Alcotest.(check time_testable) "midnight time-micros roundtrip"
    time decoded

let test_time_micros_precise () =
  let time = (14, 25, 36, 123456) in
  let encoded = Codec.encode_to_bytes time_micros time in
  let decoded = Codec.decode_from_bytes time_micros encoded in
  Alcotest.(check time_testable) "precise time-micros roundtrip"
    time decoded

(* ========== TIMESTAMP TESTS ========== *)

let test_timestamp_millis_epoch () =
  let ts = 0L in
  let encoded = Codec.encode_to_bytes timestamp_millis ts in
  let decoded = Codec.decode_from_bytes timestamp_millis encoded in
  Alcotest.(check int64) "epoch timestamp-millis roundtrip"
    ts decoded

let test_timestamp_millis_now () =
  let ts = Int64.of_float (Unix.time () *. 1000.) in
  let encoded = Codec.encode_to_bytes timestamp_millis ts in
  let decoded = Codec.decode_from_bytes timestamp_millis encoded in
  Alcotest.(check int64) "current timestamp-millis roundtrip"
    ts decoded

let test_timestamp_micros_epoch () =
  let ts = 0L in
  let encoded = Codec.encode_to_bytes timestamp_micros ts in
  let decoded = Codec.decode_from_bytes timestamp_micros encoded in
  Alcotest.(check int64) "epoch timestamp-micros roundtrip"
    ts decoded

let test_timestamp_micros_precise () =
  let ts = Int64.of_float (Unix.time () *. 1_000_000.) in
  let encoded = Codec.encode_to_bytes timestamp_micros ts in
  let decoded = Codec.decode_from_bytes timestamp_micros encoded in
  Alcotest.(check int64) "precise timestamp-micros roundtrip"
    ts decoded

(* ========== DECIMAL TESTS ========== *)

let test_decimal_zero () =
  let dec_codec = decimal ~precision:10 ~scale:2 in
  let value = Z.zero in
  let encoded = Codec.encode_to_bytes dec_codec value in
  let decoded = Codec.decode_from_bytes dec_codec encoded in
  Alcotest.(check bool) "decimal zero roundtrip"
    true (Z.equal value decoded)

let test_decimal_positive () =
  (* 99.99 stored as 9999 with scale=2 *)
  let dec_codec = decimal ~precision:10 ~scale:2 in
  let value = Z.of_int 9999 in
  let encoded = Codec.encode_to_bytes dec_codec value in
  let decoded = Codec.decode_from_bytes dec_codec encoded in
  Alcotest.(check bool) "decimal positive roundtrip"
    true (Z.equal value decoded)

let test_decimal_negative () =
  (* -123.45 stored as -12345 with scale=2 *)
  let dec_codec = decimal ~precision:10 ~scale:2 in
  let value = Z.of_int (-12345) in
  let encoded = Codec.encode_to_bytes dec_codec value in
  let decoded = Codec.decode_from_bytes dec_codec encoded in
  Alcotest.(check bool) "decimal negative roundtrip"
    true (Z.equal value decoded)

let test_decimal_large () =
  (* Large number *)
  let dec_codec = decimal ~precision:20 ~scale:4 in
  let value = Z.of_string "123456789012345678" in
  let encoded = Codec.encode_to_bytes dec_codec value in
  let decoded = Codec.decode_from_bytes dec_codec encoded in
  Alcotest.(check bool) "decimal large roundtrip"
    true (Z.equal value decoded)

let test_decimal_fixed_positive () =
  let dec_codec = decimal_fixed ~precision:10 ~scale:2 ~size:8 in
  let value = Z.of_int 9999 in
  let encoded = Codec.encode_to_bytes dec_codec value in
  let decoded = Codec.decode_from_bytes dec_codec encoded in
  Alcotest.(check bool) "decimal-fixed positive roundtrip"
    true (Z.equal value decoded)

let test_decimal_fixed_negative () =
  let dec_codec = decimal_fixed ~precision:10 ~scale:2 ~size:8 in
  let value = Z.of_int (-12345) in
  let encoded = Codec.encode_to_bytes dec_codec value in
  let decoded = Codec.decode_from_bytes dec_codec encoded in
  Alcotest.(check bool) "decimal-fixed negative roundtrip"
    true (Z.equal value decoded)

(* ========== UUID TESTS ========== *)

let test_uuid_roundtrip () =
  let uid = Uuidm.v4_gen (Random.State.make_self_init ()) () in
  let encoded = Codec.encode_to_bytes uuid uid in
  let decoded = Codec.decode_from_bytes uuid encoded in
  Alcotest.(check bool) "uuid roundtrip"
    true (Uuidm.equal uid decoded)

let test_uuid_nil () =
  let uid = Uuidm.nil in
  let encoded = Codec.encode_to_bytes uuid uid in
  let decoded = Codec.decode_from_bytes uuid encoded in
  Alcotest.(check bool) "uuid nil roundtrip"
    true (Uuidm.equal uid decoded)

(* ========== DURATION TESTS ========== *)

let test_duration_zero () =
  let dur = { months = 0; days = 0; millis = 0 } in
  let encoded = Codec.encode_to_bytes duration dur in
  let decoded = Codec.decode_from_bytes duration encoded in
  Alcotest.(check int) "duration zero months" dur.months decoded.months;
  Alcotest.(check int) "duration zero days" dur.days decoded.days;
  Alcotest.(check int) "duration zero millis" dur.millis decoded.millis

let test_duration_normal () =
  let dur = { months = 6; days = 15; millis = 5000 } in
  let encoded = Codec.encode_to_bytes duration dur in
  let decoded = Codec.decode_from_bytes duration encoded in
  Alcotest.(check int) "duration months" dur.months decoded.months;
  Alcotest.(check int) "duration days" dur.days decoded.days;
  Alcotest.(check int) "duration millis" dur.millis decoded.millis

let test_duration_large () =
  let dur = { months = 120; days = 365; millis = 86400000 } in
  let encoded = Codec.encode_to_bytes duration dur in
  let decoded = Codec.decode_from_bytes duration encoded in
  Alcotest.(check int) "duration large months" dur.months decoded.months;
  Alcotest.(check int) "duration large days" dur.days decoded.days;
  Alcotest.(check int) "duration large millis" dur.millis decoded.millis

(* ========== CUSTOM LOGICAL TYPE TESTS ========== *)

(* Custom money type that stores dollars as integer cents *)
type money = { dollars: int; cents: int }

let money_to_cents m = m.dollars * 100 + m.cents
let cents_to_money c =
  { dollars = c / 100; cents = c mod 100 }

let money_codec =
  make_logical
    ~name:"money-cents"
    ~base_codec:Codec.int
    ~encode:money_to_cents
    ~decode:cents_to_money

let test_custom_money_cents () =
  let amount = { dollars = 42; cents = 50 } in
  let encoded = Codec.encode_to_bytes money_codec amount in
  let decoded = Codec.decode_from_bytes money_codec encoded in
  Alcotest.(check int) "custom money dollars" amount.dollars decoded.dollars;
  Alcotest.(check int) "custom money cents" amount.cents decoded.cents

(* ========== CONTAINER FILE INTEGRATION TESTS ========== *)

let test_logical_in_container () =
  (* Test that logical types work in container files *)
  let filename = Filename.temp_file "avro_logical_test" ".avro" in
  Fun.protect ~finally:(fun () -> Sys.remove filename) (fun () ->
    (* Write with UUID logical type *)
    let uid1 = Uuidm.v4_gen (Random.State.make_self_init ()) () in
    let uid2 = Uuidm.v4_gen (Random.State.make_self_init ()) () in

    let writer = Container_writer.create ~path:filename ~codec:uuid () in
    Container_writer.write writer uid1;
    Container_writer.write writer uid2;
    Container_writer.close writer;

    (* Read back *)
    let reader = Container_reader.open_file ~path:filename ~codec:uuid () in
    let uids = Container_reader.fold (fun acc v -> v :: acc) [] reader in
    Container_reader.close reader;
    let uids = List.rev uids in
    Alcotest.(check int) "container uuid count" 2 (List.length uids);
    Alcotest.(check bool) "container uuid 1" true (Uuidm.equal uid1 (List.nth uids 0));
    Alcotest.(check bool) "container uuid 2" true (Uuidm.equal uid2 (List.nth uids 1))
  )

let test_logical_date_container () =
  (* Test dates in container files *)
  let filename = Filename.temp_file "avro_date_test" ".avro" in
  Fun.protect ~finally:(fun () -> Sys.remove filename) (fun () ->
    let dates = [
      (1970, 1, 1);
      (2024, 11, 13);
      (2000, 2, 29);  (* Leap year *)
    ] in

    let writer = Container_writer.create ~path:filename ~codec:date () in
    List.iter (Container_writer.write writer) dates;
    Container_writer.close writer;

    let reader = Container_reader.open_file ~path:filename ~codec:date () in
    let decoded_dates = Container_reader.fold (fun acc v -> v :: acc) [] reader in
    Container_reader.close reader;
    let decoded_dates = List.rev decoded_dates in
    Alcotest.(check int) "container date count" 3 (List.length decoded_dates);
    List.iteri (fun i expected_date ->
      Alcotest.(check (triple int int int)) (Printf.sprintf "date %d" i)
        expected_date (List.nth decoded_dates i)
    ) dates
  )

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Logical Types" [
    "date", [
      test_case "epoch" `Quick test_date_epoch;
      test_case "recent" `Quick test_date_recent;
      test_case "pre-epoch" `Quick test_date_pre_epoch;
      test_case "leap year" `Quick test_date_leap_year;
    ];

    "time-millis", [
      test_case "midnight" `Quick test_time_millis_midnight;
      test_case "noon" `Quick test_time_millis_noon;
    ];

    "time-micros", [
      test_case "midnight" `Quick test_time_micros_midnight;
      test_case "precise" `Quick test_time_micros_precise;
    ];

    "timestamps", [
      test_case "timestamp-millis epoch" `Quick test_timestamp_millis_epoch;
      test_case "timestamp-millis now" `Quick test_timestamp_millis_now;
      test_case "timestamp-micros epoch" `Quick test_timestamp_micros_epoch;
      test_case "timestamp-micros precise" `Quick test_timestamp_micros_precise;
    ];

    "decimal", [
      test_case "zero" `Quick test_decimal_zero;
      test_case "positive" `Quick test_decimal_positive;
      test_case "negative" `Quick test_decimal_negative;
      test_case "large" `Quick test_decimal_large;
    ];

    "decimal-fixed", [
      test_case "positive" `Quick test_decimal_fixed_positive;
      test_case "negative" `Quick test_decimal_fixed_negative;
    ];

    "uuid", [
      test_case "roundtrip" `Quick test_uuid_roundtrip;
      test_case "nil" `Quick test_uuid_nil;
    ];

    "duration", [
      test_case "zero" `Quick test_duration_zero;
      test_case "normal" `Quick test_duration_normal;
      test_case "large" `Quick test_duration_large;
    ];

    "custom", [
      test_case "money-cents" `Quick test_custom_money_cents;
    ];

    "container integration", [
      test_case "uuid in container" `Quick test_logical_in_container;
      test_case "date in container" `Quick test_logical_date_container;
    ];
  ]
