# OCaml Avro

A pure OCaml implementation of [Apache Avro](https://avro.apache.org/docs/1.11.1/specification/) with codec-based design, schema evolution support, and container file format.

## Features

- **Value-centric design**: Manual codec construction using combinators (no code generation required)
- **Pure OCaml**: No external C dependencies for core functionality
- **Schema evolution**: Built-in support for reading data with different schemas
- **Container files**: Full support for Avro Object Container File format
- **Compression**: Multiple compression codecs (null, deflate, snappy, zstandard)
- **Type-safe**: Codec-enforced types with composable combinators

## Quick Start

### Example Usage

```ocaml
open Avro

(* Encode a string *)
let name = "Alice" in
let encoded = Codec.encode_to_bytes Codec.string name in
let decoded = Codec.decode_from_bytes Codec.string encoded in
assert (name = decoded)

(* Encode an array *)
let numbers = [| 1; 2; 3; 4; 5 |] in
let codec = Codec.array Codec.int in
let encoded = Codec.encode_to_bytes codec numbers in
let decoded = Codec.decode_from_bytes codec encoded in
assert (numbers = decoded)

(* Optional values *)
let codec = Codec.option Codec.string in
let some_value = Some "hello" in
let encoded = Codec.encode_to_bytes codec some_value in
let decoded = Codec.decode_from_bytes codec encoded in
assert (some_value = decoded)
```

Check out `examples/basic_usage.ml` for a working demonstration.

## Compression Codecs

### Using Built-in Codecs

Container files support compression with automatic codec detection:

```ocaml
open Avro

(* Write compressed container file *)
let write_compressed () =
  let codec = Codec.array Codec.int in
  let writer = Container_writer.create ~compression:"zstandard" codec "data.avro" in
  Container_writer.write writer [|1; 2; 3; 4; 5|];
  Container_writer.close writer

(* Read compressed container file - codec auto-detected *)
let read_compressed () =
  let codec = Codec.array Codec.int in
  match Container_reader.read_all codec "data.avro" with
  | Ok data -> Array.iter (Printf.printf "%d ") data
  | Error msg -> Printf.eprintf "Error: %s\n" msg
```

### Available Codecs

```ocaml
(* List all available compression codecs *)
let codecs = Codec_registry.list () in
List.iter print_endline codecs
(* Output (with all optional deps installed):
   null
   deflate
   zstandard
   snappy
*)

(* Check if a specific codec is available *)
match Codec_registry.get "zstandard" with
| Some _ -> print_endline "Zstandard available!"
| None -> print_endline "Zstandard not installed"
```

### Registering Custom Codecs

You can register your own compression codec:

```ocaml
module My_LZ4_Codec = struct
  type t = unit
  let name = "lz4"
  let create () = ()
  let compress () data = Lz4.compress data
  let decompress () data = Lz4.decompress data
end

(* Register the codec *)
let () = Codec_registry.register "lz4" (module My_LZ4_Codec)

(* Use it in container files *)
let writer = Container_writer.create ~compression:"lz4" codec "data.avro" in
...
```

**Compression Performance Tips:**

- `null`: Use for already-compressed data or when speed is critical
- `deflate`: Good default, widely compatible, moderate compression
- `zstandard`: Best compression ratio, fast decompression (recommended)
- `snappy`: Fastest compression, lower ratio (good for real-time data)

## Building

```bash
# Build the library
dune build src

# Build examples
dune build examples

# Build everything (requires test dependencies)
dune build

# Run tests (requires alcotest and qcheck)
dune runtest

# Run benchmarks (requires benchmark library)
dune build @bench/runbench

# Watch mode (rebuild on save)
dune build --watch

# Clean build artifacts
dune clean
```


## Installation

### Basic Installation

```bash
# Install with deflate compression support (recommended)
opam install avro

# Or install from source
git clone https://github.com/tmcgilchrist/ocaml-avro.git
cd ocaml-avro/avro-simple
opam install . --deps-only
dune build
dune install
```

### With Optional Compression Codecs

The library supports optional compression codecs that are automatically enabled when their dependencies are installed:

```bash
# Install with Zstandard support (recommended for modern deployments)
opam install avro zstd

# Install with Snappy support (common in Hadoop/Spark ecosystems)
opam install avro snappy

# Install with all compression codecs
opam install avro zstd snappy
```

**Available Codecs:**

| Codec       | Package           | Status         | Use Case                 |
|-------------|-------------------|----------------|--------------------------|
| `null`      | Always available  | ✅             | No compression           |
| `deflate`   | Always available  | ✅             | DEFLATE/GZIP (default)   |
| `zstandard` | `zstd >= 0.3`     | ⭐ Recommended | Modern, excellent ratio  |
| `snappy`    | `snappy >= 0.1.0` | ✅ Optional    | Fast, big data pipelines |


See [INSTALL.md](INSTALL.md) for detailed installation instructions.


## Acknowledgements and Alternatives

This library started as a port of [avro-simple](https://github.com/icicle-lang/avro-simple) from Haskell, when I needed to process Avro files in OCaml. Hopefully it feels native-OCaml style. I also referenced the ocaml-avro library by c-cube, below I provide a detailed comparison to that library. If I've missed a point or something is inaccurate please make a PR correcting it.

## Comparison: avro-simple vs ocaml-avro

This document compares the two OCaml Avro library implementations to help you choose the right one for your project.

### Overview

| Feature               | avro-simple (this library)                                            | ocaml-avro (c-cube)                                       |
|-----------------------|-----------------------------------------------------------------------|-----------------------------------------------------------|
| **Repository**        | [tmcgilchrist/ocaml-avro](https://github.com/tmcgilchrist/ocaml-avro) | [c-cube/ocaml-avro](https://github.com/c-cube/ocaml-avro) |
| **Design Philosophy** | Value-centric, codec-based library                                    | Schema-first code generation                              |
| **OCaml Version**     | 5.0+                                                                  | 4.08+                                                     |
| **Pure OCaml**        | ✅ Yes (optional C deps for compression)                              | ✅ Yes                                                    |

### Core Differences

### Design Approach

| Aspect               | avro-simple                               | ocaml-avro                       |
|----------------------|-------------------------------------------|----------------------------------|
| **Code Generation**  | ❌ Not required                           | ✅ Required (avro-compiler)      |
| **Schema Handling**  | Dynamic, runtime codecs                   | Static, compile-time types       |
| **Type Definitions** | Manual codec construction via combinators | Generated from JSON schemas      |
| **Schema Evolution** | ✅ Built-in, first-class support          | ⚠️ Limited                        |
| **Dynamic Schemas**  | ✅ Supported                              | ❌ Not supported                 |
| **Build Complexity** | Simple (library only)                     | Requires code generator in build |

### Feature Comparison

### Schema & Type System

| Feature                 | avro-simple                                       | ocaml-avro                       |
|-------------------------|---------------------------------------------------|----------------------------------|
| **Primitive Types**     | ✅ All types                                      | ✅ All types                     |
| **Complex Types**       | ✅ Records, arrays, maps, unions                  | ✅ Records, arrays, maps, unions |
| **Enums**               | ✅ Supported                                      | ✅ Supported                     |
| **Fixed**               | ✅ Supported                                      | ✅ Supported                     |
| **Recursive Types**     | ✅ Supported (fixpoint-based)                     | ✅ Supported                     |
| **Logical Types**       | ✅ Full support (date, time, decimal, uuid, etc.) | ⚠️ Partial support                |
| **Aliases**             | ✅ Type and field aliases                         | ❌ Not supported                 |
| **Schema Validation**   | ✅ Full validation                                | ⚠️ Basic validation               |
| **JSON Schema Parsing** | ✅ Bidirectional (parse & generate)               | ✅ Parse only                    |
| **Canonical Form**      | ✅ Supported                                      | ✅ Supported                     |

### Schema Evolution

| Feature                   | avro-simple                     | ocaml-avro          |
|---------------------------|---------------------------------|---------------------|
| **Reader/Writer Schemas** | ✅ Full deconflict algorithm    | ⚠️ Limited           |
| **Type Promotions**       | ✅ int→long→float→double        | ❌ Not supported    |
| **Field Reordering**      | ✅ Automatic                    | ❌ Must match order |
| **Field Defaults**        | ✅ Supported                    | ⚠️ Limited           |
| **Union Evolution**       | ✅ Both-unions, union promotion | ❌ Not supported    |
| **Enum Evolution**        | ✅ Symbol mapping               | ❌ Not supported    |
| **Field Aliases**         | ✅ Supported                    | ❌ Not supported    |
| **Type Aliases**          | ✅ Supported                    | ❌ Not supported    |

### Container Files

| Feature               | avro-simple                  | ocaml-avro         |
|-----------------------|------------------------------|--------------------|
| **OCF Support**       | ✅ Full read/write           | ✅ Full read/write |
| **Sync Markers**      | ✅ Supported                 | ✅ Supported       |
| **Block Compression** | ✅ Supported                 | ✅ Supported       |
| **Metadata**          | ✅ Read/write                | ✅ Read/write      |
| **Streaming**         | ✅ Iterator/fold/sequence    | ✅ Iterator        |
| **Writer Schema**     | ✅ Auto-parsed from metadata | ✅ Supported       |

### Compression Codecs

| Codec             | avro-simple              | ocaml-avro       |
|-------------------|--------------------------|------------------|
| **null**          | ✅ Built-in              | ✅ Built-in      |
| **deflate**       | ✅ Built-in (decompress) | ✅ Built-in      |
| **snappy**        | ✅ Optional (snappy pkg) | ❌ Not supported |
| **zstandard**     | ✅ Optional (zstd pkg)   | ❌ Not supported |
| **Custom Codecs** | ✅ Registry system       | ❌ Not supported |


### Use Cases

### Choose avro-simple when you need:

✅ **Dynamic schema handling** - Working with schemas determined at runtime
✅ **Schema evolution** - Reading data written with different schema versions
✅ **No build complexity** - Simple library dependency, no code generation step
✅ **Flexible data processing** - Building ETL pipelines, data transformations
✅ **Modern compression** - Snappy or Zstandard for better performance
✅ **Composable codecs** - Building complex types from simpler ones
✅ **Logical types** - Full support for date, time, decimal, uuid

**Example scenarios:**
- Data migration tools that handle multiple schema versions
- Log processing systems with evolving schemas
- Microservices with schema registry integration
- Interactive data exploration tools

### Choose ocaml-avro when you need:

✅ **Compile-time safety** - Catch schema errors at compile time
✅ **Static schemas** - Schemas known and fixed at build time
✅ **Simpler types** - Generated types match your mental model exactly
✅ **Minimal runtime** - No codec setup overhead
✅ **Legacy support** - Support for older OCaml versions (4.08+)

**Example scenarios:**
- Fixed-schema data storage with known structure
- Performance-critical applications with static schemas
- Projects with existing schema files to generate from
- Simple serialization without schema evolution needs

