(* Test program to figure out Gzip API *)

let () =
  let test_string = "Hello, World! This is a test of GZIP compression." in

  (* Try different potential APIs *)
  try
    (* Try method 1: Gzip module with compress/uncompress *)
    let compressed = Gzip.compress (Bytes.of_string test_string) in
    let decompressed = Gzip.uncompress compressed in
    Printf.printf "Method 1 works! Compressed %d -> %d bytes\n"
      (String.length test_string) (Bytes.length compressed);
    Printf.printf "Decompressed: %s\n" (Bytes.to_string decompressed)
  with _ ->
    Printf.printf "Method 1 failed, trying others...\n";

    (* Will discover the API through compilation errors *)
    ()
