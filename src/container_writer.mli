(** Writer for Avro Object Container Files.

    This module provides functionality to write Avro Object Container Files,
    which are self-describing files that store a collection of Avro-encoded
    objects along with their schema.

    {1 Avro Object Container File Format}

    An Avro Object Container File consists of:
    - A file header containing magic bytes, metadata (including schema), and a sync marker
    - One or more data blocks, each containing serialized objects and the sync marker
    - Each data block stores a count of objects, the size of compressed data, and the serialized objects

    {1 Writing Process}

    The typical workflow is:
    1. Create a writer with {!create}, specifying the schema codec and options
    2. Write objects using {!write} (buffered) or {!write_block} (immediate)
    3. Optionally {!flush} to ensure all buffered data is written
    4. Close the writer with {!close} when done

    {1 Compression}

    The writer supports pluggable compression codecs. The compression codec name
    is stored in the file metadata and must match a codec registered in the
    {!Codec_registry}. The "null" codec (no compression) is always available.

    {1 Sync Markers}

    Sync markers are 16-byte random values written after each data block. They
    allow readers to efficiently seek within the file and recover from corrupted
    blocks by searching for the next sync marker.
*)

(** The type of a container file writer.

    A writer maintains:
    - An output channel to the file
    - The codec for encoding objects of type ['a]
    - Compression settings and function
    - A sync marker for data block boundaries
    - A buffer of pending objects to be written
    - Configuration for when to flush blocks

    The type parameter ['a] represents the type of objects being written.
*)
type 'a t = {
  channel: out_channel;
  codec: 'a Codec.t;
  compression: string;
  compress: bytes -> bytes;
  sync_marker: bytes;
  sync_interval: int;
  mutable buffer: 'a list;
  mutable objects_written: int;
}

(** Generate a random 16-byte sync marker.

    Creates a cryptographically random 16-byte sequence to serve as the
    sync marker for the container file. This marker is written after each
    data block and helps readers identify block boundaries.

    @return A 16-byte random sync marker
*)
val generate_sync_marker : unit -> bytes

(** Write the Avro container file header.

    The header consists of:
    - Magic bytes: "Obj\x01" (4 bytes)
    - File metadata as an Avro map containing:
      - "avro.schema": The schema in JSON format
      - "avro.codec": The compression codec name
      - Additional user-provided metadata entries
    - The 16-byte sync marker

    @param channel The output channel to write to
    @param schema The Avro schema for the objects in this file
    @param compression The name of the compression codec
    @param metadata Additional metadata key-value pairs to include
    @param sync_marker The 16-byte sync marker for this file
*)
val write_header : out_channel -> Schema.t -> string -> (string * string) list -> bytes -> unit

(** Create a new container file writer.

    Opens a binary file for writing and initializes it with the Avro container
    file header. The writer buffers objects and writes them in blocks when the
    buffer reaches the sync interval size.

    @param path The filesystem path where the container file will be created
    @param codec The codec for encoding objects of type ['a]
    @param compression The compression codec name (default: "null" for no compression)
    @param metadata Additional metadata key-value pairs to include in the file header (default: [])
    @param sync_interval Number of objects to buffer before writing a block (default: 4000)
    @return A new container file writer

    @raise Failure if the compression codec is not registered in {!Codec_registry}
    @raise Sys_error if the file cannot be opened for writing
*)
val create : path:string -> codec:'a Codec.t -> ?compression:string -> ?metadata:(string * string) list -> ?sync_interval:int -> unit -> 'a t

(** Write a data block to the container file.

    Serializes the buffered objects, compresses them (if compression is enabled),
    and writes a complete data block consisting of:
    - Object count (Avro long)
    - Compressed data size in bytes (Avro long)
    - Compressed serialized objects
    - Sync marker (16 bytes)

    After writing, the buffer is cleared. If the buffer is empty, this is a no-op.

    @param t The container file writer
*)
val flush_block : 'a t -> unit

(** Write a single value to the container file.

    Adds the value to the internal buffer. When the buffer size reaches the
    sync interval, the block is automatically flushed using {!flush_block}.

    For better performance when writing many objects, consider using {!write_block}
    to write multiple objects at once.

    @param t The container file writer
    @param value The value to write
*)
val write : 'a t -> 'a -> unit

(** Write multiple values as a single block.

    Flushes any pending buffered data, then writes all the provided values
    as a single data block. This is more efficient than calling {!write}
    multiple times when you have a batch of objects to write.

    @param t The container file writer
    @param values An array of values to write as a block
*)
val write_block : 'a t -> 'a array -> unit

(** Flush any pending buffered data.

    Writes any objects remaining in the buffer as a data block, then flushes
    the underlying output channel to ensure all data is written to disk.

    It's not necessary to call this before {!close}, but it can be useful
    to ensure data is persisted at specific points.

    @param t The container file writer
*)
val flush : 'a t -> unit

(** Close the container file writer.

    Flushes any pending buffered data and closes the underlying output channel.
    After calling this function, the writer cannot be used anymore.

    @param t The container file writer
*)
val close : 'a t -> unit
