(** Tests for record codec builder *)

open Avro_simple

(* ========== SIMPLE RECORD ========== *)

type person = {
  name: string;
  age: int;
}

let person_codec =
  Codec.record (Type_name.simple "Person") (fun name age -> { name; age })
  |> Codec.field "name" Codec.string (fun p -> p.name)
  |> Codec.field "age" Codec.int (fun p -> p.age)
  |> Codec.finish

let test_simple_record_roundtrip () =
  let person = { name = "Alice"; age = 30 } in
  let encoded = Codec.encode_to_bytes person_codec person in
  let decoded = Codec.decode_from_bytes person_codec encoded in
  Alcotest.(check string) "name matches" person.name decoded.name;
  Alcotest.(check int) "age matches" person.age decoded.age

let test_simple_record_schema () =
  match person_codec.schema with
  | Schema.Record r ->
      Alcotest.(check int) "has 2 fields" 2 (List.length r.fields);
      Alcotest.(check string) "first field is 'name'"
        "name" (List.hd r.fields).field_name
  | _ ->
      Alcotest.fail "Schema should be a record"

(* ========== RECORD WITH OPTIONAL FIELDS ========== *)

type user = {
  username: string;
  email: string option;
  age_opt: int option;
}

let user_codec =
  Codec.record (Type_name.simple "User")
    (fun username email age_opt -> { username; email; age_opt })
  |> Codec.field "username" Codec.string (fun u -> u.username)
  |> Codec.field_opt "email" Codec.string (fun u -> u.email)
  |> Codec.field_opt "age" Codec.int (fun u -> u.age_opt)
  |> Codec.finish

let test_record_with_some_fields () =
  let user = { username = "bob"; email = Some "bob@example.com"; age_opt = Some 25 } in
  let encoded = Codec.encode_to_bytes user_codec user in
  let decoded = Codec.decode_from_bytes user_codec encoded in
  Alcotest.(check string) "username matches" user.username decoded.username;
  Alcotest.(check (option string)) "email matches" user.email decoded.email;
  Alcotest.(check (option int)) "age matches" user.age_opt decoded.age_opt

let test_record_with_none_fields () =
  let user = { username = "charlie"; email = None; age_opt = None } in
  let encoded = Codec.encode_to_bytes user_codec user in
  let decoded = Codec.decode_from_bytes user_codec encoded in
  Alcotest.(check string) "username matches" user.username decoded.username;
  Alcotest.(check (option string)) "email is None" user.email decoded.email;
  Alcotest.(check (option int)) "age is None" user.age_opt decoded.age_opt

(* ========== RECORD WITH VARIOUS TYPES ========== *)

type data_record = {
  id: int64;
  value: float;
  data: bytes;
  active: bool;
}

let data_codec =
  Codec.record (Type_name.simple "Data")
    (fun id value data active -> { id; value; data; active })
  |> Codec.field "id" Codec.long (fun d -> d.id)
  |> Codec.field "value" Codec.double (fun d -> d.value)
  |> Codec.field "data" Codec.bytes (fun d -> d.data)
  |> Codec.field "active" Codec.boolean (fun d -> d.active)
  |> Codec.finish

let test_record_with_various_types () =
  let record = {
    id = 12345L;
    value = 3.14159;
    data = Bytes.of_string "test data";
    active = true;
  } in
  let encoded = Codec.encode_to_bytes data_codec record in
  let decoded = Codec.decode_from_bytes data_codec encoded in
  Alcotest.(check int64) "id matches" record.id decoded.id;
  Alcotest.(check bool) "active matches" record.active decoded.active;
  let bytes_equal = Bytes.equal record.data decoded.data in
  Alcotest.(check bool) "data matches" true bytes_equal

(* ========== EMPTY RECORD (edge case) ========== *)

(* Note: Avro spec requires at least one field, but let's test single field *)
type single_field = {
  value: string;
}

let single_field_codec =
  Codec.record (Type_name.simple "SingleField") (fun value -> { value })
  |> Codec.field "value" Codec.string (fun s -> s.value)
  |> Codec.finish

let test_single_field_record () =
  let record = { value = "test" } in
  let encoded = Codec.encode_to_bytes single_field_codec record in
  let decoded = Codec.decode_from_bytes single_field_codec encoded in
  Alcotest.(check string) "value matches" record.value decoded.value

(* ========== RECORD WITH ARRAYS ========== *)

type array_record = {
  numbers: int array;
  names: string array;
}

let array_record_codec =
  Codec.record (Type_name.simple "ArrayRecord")
    (fun numbers names -> { numbers; names })
  |> Codec.field "numbers" (Codec.array Codec.int) (fun r -> r.numbers)
  |> Codec.field "names" (Codec.array Codec.string) (fun r -> r.names)
  |> Codec.finish

let test_record_with_arrays () =
  let record = {
    numbers = [| 1; 2; 3; 4; 5 |];
    names = [| "Alice"; "Bob"; "Charlie" |];
  } in
  let encoded = Codec.encode_to_bytes array_record_codec record in
  let decoded = Codec.decode_from_bytes array_record_codec encoded in
  Alcotest.(check (array int)) "numbers match" record.numbers decoded.numbers;
  Alcotest.(check (array string)) "names match" record.names decoded.names

(* ========== NESTED RECORDS ========== *)

type address = {
  street: string;
  city: string;
  zip: string;
}

let address_codec =
  Codec.record (Type_name.simple "Address")
    (fun street city zip -> { street; city; zip })
  |> Codec.field "street" Codec.string (fun a -> a.street)
  |> Codec.field "city" Codec.string (fun a -> a.city)
  |> Codec.field "zip" Codec.string (fun a -> a.zip)
  |> Codec.finish

type person_with_address = {
  name: string;
  age: int;
  address: address;
}

let person_with_address_codec =
  Codec.record (Type_name.simple "PersonWithAddress")
    (fun name age address -> { name; age; address })
  |> Codec.field "name" Codec.string (fun p -> p.name)
  |> Codec.field "age" Codec.int (fun p -> p.age)
  |> Codec.field "address" address_codec (fun p -> p.address)
  |> Codec.finish

let test_nested_record () =
  let address = { street = "123 Main St"; city = "Springfield"; zip = "12345" } in
  let person = { name = "Alice"; age = 30; address } in
  let encoded = Codec.encode_to_bytes person_with_address_codec person in
  let decoded = Codec.decode_from_bytes person_with_address_codec encoded in
  Alcotest.(check string) "name matches" person.name decoded.name;
  Alcotest.(check int) "age matches" person.age decoded.age;
  Alcotest.(check string) "street matches" person.address.street decoded.address.street;
  Alcotest.(check string) "city matches" person.address.city decoded.address.city;
  Alcotest.(check string) "zip matches" person.address.zip decoded.address.zip

(* ========== DEEPLY NESTED RECORDS ========== *)

type coordinates = {
  lat: float;
  lon: float;
}

let coordinates_codec =
  Codec.record (Type_name.simple "Coordinates")
    (fun lat lon -> { lat; lon })
  |> Codec.field "lat" Codec.double (fun c -> c.lat)
  |> Codec.field "lon" Codec.double (fun c -> c.lon)
  |> Codec.finish

type location = {
  name: string;
  coords: coordinates;
}

let location_codec =
  Codec.record (Type_name.simple "Location")
    (fun name coords -> { name; coords })
  |> Codec.field "name" Codec.string (fun l -> l.name)
  |> Codec.field "coords" coordinates_codec (fun l -> l.coords)
  |> Codec.finish

type venue = {
  venue_name: string;
  location: location;
  capacity: int;
}

let venue_codec =
  Codec.record (Type_name.simple "Venue")
    (fun venue_name location capacity -> { venue_name; location; capacity })
  |> Codec.field "venue_name" Codec.string (fun v -> v.venue_name)
  |> Codec.field "location" location_codec (fun v -> v.location)
  |> Codec.field "capacity" Codec.int (fun v -> v.capacity)
  |> Codec.finish

let test_deeply_nested_record () =
  let coords = { lat = 37.7749; lon = -122.4194 } in
  let location = { name = "San Francisco"; coords } in
  let venue = { venue_name = "Chase Center"; location; capacity = 18064 } in
  let encoded = Codec.encode_to_bytes venue_codec venue in
  let decoded = Codec.decode_from_bytes venue_codec encoded in
  Alcotest.(check string) "venue name matches" venue.venue_name decoded.venue_name;
  Alcotest.(check int) "capacity matches" venue.capacity decoded.capacity;
  Alcotest.(check string) "location name matches" venue.location.name decoded.location.name;
  (* Use approximate float comparison *)
  let lat_close = abs_float (venue.location.coords.lat -. decoded.location.coords.lat) < 0.0001 in
  let lon_close = abs_float (venue.location.coords.lon -. decoded.location.coords.lon) < 0.0001 in
  Alcotest.(check bool) "latitude matches" true lat_close;
  Alcotest.(check bool) "longitude matches" true lon_close

(* ========== RECORDS WITH OPTIONAL NESTED FIELDS ========== *)

type company = {
  company_name: string;
  headquarters: address option;
  employees: int;
}

let company_codec =
  Codec.record (Type_name.simple "Company")
    (fun company_name headquarters employees -> { company_name; headquarters; employees })
  |> Codec.field "company_name" Codec.string (fun c -> c.company_name)
  |> Codec.field_opt "headquarters" address_codec (fun c -> c.headquarters)
  |> Codec.field "employees" Codec.int (fun c -> c.employees)
  |> Codec.finish

let test_nested_optional_some () =
  let hq = { street = "1 Infinite Loop"; city = "Cupertino"; zip = "95014" } in
  let company = { company_name = "Acme Corp"; headquarters = Some hq; employees = 1000 } in
  let encoded = Codec.encode_to_bytes company_codec company in
  let decoded = Codec.decode_from_bytes company_codec encoded in
  Alcotest.(check string) "company name matches" company.company_name decoded.company_name;
  Alcotest.(check int) "employees match" company.employees decoded.employees;
  match decoded.headquarters with
  | None -> Alcotest.fail "Expected Some headquarters"
  | Some addr ->
      Alcotest.(check string) "hq street matches" hq.street addr.street;
      Alcotest.(check string) "hq city matches" hq.city addr.city;
      Alcotest.(check string) "hq zip matches" hq.zip addr.zip

let test_nested_optional_none () =
  let company = { company_name = "Remote Inc"; headquarters = None; employees = 50 } in
  let encoded = Codec.encode_to_bytes company_codec company in
  let decoded = Codec.decode_from_bytes company_codec encoded in
  Alcotest.(check string) "company name matches" company.company_name decoded.company_name;
  Alcotest.(check int) "employees match" company.employees decoded.employees;
  Alcotest.(check (option (Alcotest.of_pp (fun fmt _ -> Format.fprintf fmt "address"))))
    "headquarters is None" None decoded.headquarters

(* ========== RECORDS WITH ARRAYS OF NESTED RECORDS ========== *)

type team = {
  team_name: string;
  members: person array;
}

let team_codec =
  Codec.record (Type_name.simple "Team")
    (fun team_name members -> { team_name; members })
  |> Codec.field "team_name" Codec.string (fun t -> t.team_name)
  |> Codec.field "members" (Codec.array person_codec) (fun t -> t.members)
  |> Codec.finish

let test_array_of_nested_records () =
  let members : person array = [|
    { name = "Alice"; age = 30 };
    { name = "Bob"; age = 25 };
    { name = "Charlie"; age = 35 };
  |] in
  let team = { team_name = "Engineering"; members } in
  let encoded = Codec.encode_to_bytes team_codec team in
  let decoded = Codec.decode_from_bytes team_codec encoded in
  Alcotest.(check string) "team name matches" team.team_name decoded.team_name;
  Alcotest.(check int) "member count matches" (Array.length members) (Array.length decoded.members);
  Array.iteri (fun i (orig_member : person) ->
    Alcotest.(check string) (Printf.sprintf "member %d name" i) orig_member.name decoded.members.(i).name;
    Alcotest.(check int) (Printf.sprintf "member %d age" i) orig_member.age decoded.members.(i).age
  ) members

(* ========== RECORDS WITH MAPS OF NESTED RECORDS ========== *)

type department = {
  dept_name: string;
  teams: (string * team) list;  (* Map represented as association list *)
}

let department_codec =
  Codec.record (Type_name.simple "Department")
    (fun dept_name teams -> { dept_name; teams })
  |> Codec.field "dept_name" Codec.string (fun d -> d.dept_name)
  |> Codec.field "teams" (Codec.map team_codec) (fun d -> d.teams)
  |> Codec.finish

let test_map_of_nested_records () =
  let engineering_team = {
    team_name = "Engineering";
    members = [| { name = "Alice"; age = 30 }; { name = "Bob"; age = 25 } |]
  } in
  let design_team = {
    team_name = "Design";
    members = [| { name = "Charlie"; age = 28 } |]
  } in
  let dept = {
    dept_name = "Product Development";
    teams = [("engineering", engineering_team); ("design", design_team)]
  } in
  let encoded = Codec.encode_to_bytes department_codec dept in
  let decoded = Codec.decode_from_bytes department_codec encoded in
  Alcotest.(check string) "dept name matches" dept.dept_name decoded.dept_name;
  Alcotest.(check int) "team count matches" (List.length dept.teams) (List.length decoded.teams);
  (* Verify engineering team *)
  let eng_team = List.assoc "engineering" decoded.teams in
  Alcotest.(check string) "eng team name" "Engineering" eng_team.team_name;
  Alcotest.(check int) "eng member count" 2 (Array.length eng_team.members);
  (* Verify design team *)
  let design_team_decoded = List.assoc "design" decoded.teams in
  Alcotest.(check string) "design team name" "Design" design_team_decoded.team_name;
  Alcotest.(check int) "design member count" 1 (Array.length design_team_decoded.members)

(* ========== TEST SUITE ========== *)

let () =
  let open Alcotest in
  run "Records" [
    "simple records", [
      test_case "roundtrip" `Quick test_simple_record_roundtrip;
      test_case "schema" `Quick test_simple_record_schema;
      test_case "single field" `Quick test_single_field_record;
    ];

    "optional fields", [
      test_case "with Some values" `Quick test_record_with_some_fields;
      test_case "with None values" `Quick test_record_with_none_fields;
    ];

    "complex records", [
      test_case "various types" `Quick test_record_with_various_types;
      test_case "with arrays" `Quick test_record_with_arrays;
    ];

    "nested records", [
      test_case "simple nested" `Quick test_nested_record;
      test_case "deeply nested" `Quick test_deeply_nested_record;
      test_case "optional nested Some" `Quick test_nested_optional_some;
      test_case "optional nested None" `Quick test_nested_optional_none;
      test_case "array of records" `Quick test_array_of_nested_records;
      test_case "map of records" `Quick test_map_of_nested_records;
    ];
  ]
