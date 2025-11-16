(lang dune 3.19)
(name avro-simple)
(source (github tmcgilchrist/avro-simple))
(license BSD-3-clause)
(authors "Tim McGilchrist")
(maintainers "timmcgil@gmail.com")

(generate_opam_files true)

(package
 (name avro-simple)
 (synopsis "Pure OCaml implementation of Apache Avro")
 (description "A fast, feature-rich, pure OCaml implementation of Apache Avro with codec-based design, schema evolution support, and container file format.")
 (depends
  (ocaml (>= 5.0))
  (dune (>= 3.0))
  (yojson (>= 2.0.0))
  (decompress (>= 1.5.0))
  (bigstringaf (>= 0.9.0))
  (uuidm (>= 0.9))
  (zarith (>= 1.14))
  (alcotest (and :with-test (>= 1.7.0)))
  (qcheck (and :with-test (>= 0.20)))
  (avro :with-dev-setup)
  (benchmark :with-dev-setup))
 (depopts
  (snappy (>= 0.1.0))
  (zstd (>= 0.3))))
