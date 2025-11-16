(** Reader for Avro Object Container Files.

    This module provides functionality to read Avro Object Container Files (OCF),
    which is Avro's standard format for storing collections of data records.
    An OCF file consists of:
    - A header with magic bytes, metadata (including schema and compression codec),
      and a sync marker
    - A series of data blocks, each containing compressed serialized objects
      followed by the sync marker

    The reader handles:
    - Schema extraction from file metadata
    - Compression codec detection and decompression
    - Block-level reading with sync marker verification
    - Iteration over all records in the file
    - Lazy sequence generation for memory-efficient processing

    Example usage:
    {[
      let codec = Codec.create ~schema ~encode ~decode in
      let reader = Container_reader.open_file ~path:"data.avro" ~codec () in
      Container_reader.iter (fun record ->
        (* process each record *)
        print_endline (to_string record)
      ) reader;
      Container_reader.close reader
    ]}
*)

(** {1 Types} *)

type 'a t
(** The type of a container file reader.
    The type parameter ['a] represents the OCaml type that records are decoded into.

    The reader maintains:
    - An open file channel
    - The codec for decoding records
    - The writer schema from the file metadata
    - Compression settings and decompression function
    - The sync marker for block verification
    - Full metadata from the file header
*)

(** {1 Opening and Closing Files} *)

val open_file : path:string -> codec:'a Codec.t -> unit -> 'a t
(** [open_file ~path ~codec ()] opens an Avro container file at [path] for reading.

    The function performs the following operations:
    1. Opens the file in binary mode
    2. Reads and verifies the magic bytes ("Obj\x01")
    3. Parses the metadata map from the header
    4. Extracts and parses the writer schema from metadata
    5. Determines the compression codec from metadata (defaults to "null")
    6. Sets up the appropriate decompression function
    7. Reads the 16-byte sync marker

    @param path The filesystem path to the Avro container file
    @param codec The codec containing the decoder for converting bytes to ['a]
    @return A reader instance ready to read data blocks

    @raise Failure if the file has invalid magic bytes, missing schema metadata,
                    unparseable schema, or unknown compression codec
    @raise Sys_error if the file cannot be opened
*)

val close : 'a t -> unit
(** [close t] closes the underlying file channel.

    It is important to close the reader when done to release system resources.
    After closing, the reader should not be used for further operations.
*)

val of_bytes : bytes -> codec:'a Codec.t -> unit -> 'a t
(** [of_bytes data ~codec ()] creates an Avro container reader from in-memory bytes.

    This function is useful for reading Avro container data that has been loaded
    into memory (e.g., from a network request or browser FileReader API).

    The function performs the same validation and initialization as {!open_file},
    but operates entirely on the provided bytes without file I/O.

    @param data The complete Avro container file contents as bytes
    @param codec The codec containing the decoder for converting bytes to ['a]
    @return A reader instance ready to read data blocks

    @raise Failure if the data has invalid magic bytes, missing schema metadata,
                    unparseable schema, or unknown compression codec
*)

val open_at_offset : path:string -> codec:'a Codec.t -> offset:int -> 'a t
(** [open_at_offset ~path ~codec ~offset] opens a container file and seeks to
    a specific byte offset.

    This is useful for parallel processing of container files when you know
    the offset of a specific block (e.g., from an external index).
    Note: This does not validate that the offset points to a valid block boundary.

    @param path The filesystem path to the Avro container file
    @param codec The codec for decoding records
    @param offset The byte offset to seek to after opening
    @return A reader positioned at the specified offset
*)

(** {1 Metadata Access} *)

val writer_schema : 'a t -> Schema.t
(** [writer_schema t] returns the schema that was used to write the file.

    This schema is extracted from the "avro.schema" metadata field in the file header.
    For proper schema evolution support, this schema should be compared with the
    reader schema to perform schema resolution.
*)

val codec_name : 'a t -> string
(** [codec_name t] returns the name of the compression codec used in the file.

    Common values are:
    - "null" - no compression (default)
    - "deflate" - DEFLATE compression
    - "snappy" - Snappy compression
    - "bzip2" - bzip2 compression
    - "xz" - XZ compression
*)

val metadata : 'a t -> (string * string) list
(** [metadata t] returns the full metadata map from the file header.

    The metadata map contains key-value pairs including:
    - "avro.schema" - The writer schema as JSON
    - "avro.codec" - The compression codec name (if present)
    - Any custom metadata added by the writer

    @return An association list of metadata key-value pairs
*)

(** {1 Reading Data} *)

val read_block : 'a t -> 'a array option
(** [read_block t] reads a single data block from the container file.

    A data block consists of:
    1. Block count (varint long) - number of objects in the block
    2. Block size (varint long) - size in bytes of compressed data
    3. Compressed serialized objects
    4. 16-byte sync marker

    The function:
    - Reads the block count and byte count
    - Reads the compressed data
    - Verifies the sync marker matches the file's sync marker
    - Decompresses the data
    - Decodes all objects in the block using the codec's decoder

    @return [Some array] containing all decoded objects from the block,
            or [None] if end of file is reached

    @raise Failure if the sync marker doesn't match
*)

(** {1 Iteration} *)

val iter : ('a -> unit) -> 'a t -> unit
(** [iter f t] applies function [f] to each object in the container file.

    This reads all blocks sequentially and applies [f] to each decoded object.
    Objects within a block are processed in order, and blocks are processed
    in file order.

    Example:
    {[
      Container_reader.iter (fun record ->
        Printf.printf "Got record: %s\n" (to_string record)
      ) reader
    ]}

    @param f Function to apply to each object
    @param t The reader to iterate over
*)

val fold : ('acc -> 'a -> 'acc) -> 'acc -> 'a t -> 'acc
(** [fold f acc t] folds over all objects in the container file.

    Starting with accumulator [acc], applies [f] to each object and the
    current accumulator value, threading the result through all objects.

    Example:
    {[
      let count = Container_reader.fold (fun acc _ -> acc + 1) 0 reader in
      Printf.printf "Total records: %d\n" count
    ]}

    @param f Function to apply: takes accumulator and object, returns new accumulator
    @param acc Initial accumulator value
    @param t The reader to fold over
    @return Final accumulator value after processing all objects
*)

val to_seq : 'a t -> 'a Seq.t
(** [to_seq t] converts the container file to a lazy sequence of objects.

    This is useful for memory-efficient processing of large files, as objects
    are decoded on-demand rather than loading the entire file into memory.
    Blocks are read lazily as the sequence is consumed.

    Example:
    {[
      let seq = Container_reader.to_seq reader in
      seq |> Seq.take 100 |> Seq.iter process_record
    ]}

    @param t The reader to convert to a sequence
    @return A lazy sequence of decoded objects
*)

val iter_blocks : ('a array -> unit) -> 'a t -> unit
(** [iter_blocks f t] applies function [f] to each block as a whole array.

    Unlike [iter] which processes individual objects, this function passes
    entire blocks to the callback. This is useful when you want to process
    records in batches or need block-level granularity.

    Example:
    {[
      Container_reader.iter_blocks (fun block ->
        Printf.printf "Processing block with %d records\n" (Array.length block);
        Array.iter process_record block
      ) reader
    ]}

    @param f Function to apply to each block
    @param t The reader to iterate over
*)

(** {1 Advanced Operations} *)
