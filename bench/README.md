# OCaml Avro Benchmarks

Cross-language benchmarks comparing OCaml Avro implementation with official implementations in Java, Rust, and Python.

## Quick Start

```bash
# Build all benchmarks
./run_comparison.sh build

# Run encoding benchmark (10,000 records)
./run_comparison.sh run

# Run decoding benchmark
./run_comparison.sh -o decode run

# Run all operations (encode, decode, container)
./run_comparison.sh run-all

# Run with custom parameters
./run_comparison.sh -o decode -c 50000 -r 10 run
```

**For detailed decode benchmark examples, see [DECODE_BENCHMARKS.md](DECODE_BENCHMARKS.md)**

---

## Table of Contents

- [Implementations Compared](#implementations-compared)
- [Setup Instructions](#setup-instructions)
- [Running Benchmarks](#running-benchmarks)
- [Performance Results](#performance-results)
- [Implementation Notes](#implementation-notes)
- [Contributing Improvements](#contributing-improvements)
- **[Decode Benchmark Guide](DECODE_BENCHMARKS.md)** - Comprehensive decode benchmark examples

---

## Implementations Compared

We compare **9 implementations** across 4 languages, representing both official and optimized approaches:

### OCaml
- **avro-simple** (this library)
  - Direct codec without intermediate representation
  - Runtime schema handling

- **ocaml-avro 0.1** - Code generation approach
  - Generated code from schemas (like Java SpecificRecord)
  - Compile-time type safety
  - No container file support

### Java
- **Apache Avro 1.12.0 (GenericRecord)** - Official reference
  - Two modes: coldstart and warmed-up (JIT compiled)
  - GenericRecord for schema flexibility
  - Encoder/decoder reuse optimizations applied

### Rust
- **apache-avro 0.20** - Official Rust implementation
  - Uses intermediate `Value` representation
  - Full Avro feature support

- **serde_avro_fast 2.0** - High-performance alternative
  - Direct serde integration, no Value overhead
  - 8-10x faster than apache-avro
  - No container file support

### Python
- **fastavro 1.9+** - Community standard (C extensions)
  - High-performance implementation
  - Most commonly used in production

- **avro 1.11+** - Official Apache implementation (pure Python)
  - Reference implementation
  - 4-6x slower than fastavro

---

## Setup Instructions

### Prerequisites

**Required:**
- **OCaml 5.0+** with dune
- **hyperfine** - Benchmark tool
  ```bash
  # macOS
  brew install hyperfine

  # Linux
  cargo install hyperfine
  # or
  apt install hyperfine  # Debian/Ubuntu
  ```

**Optional (for specific benchmarks):**
- **Java 11+** with Maven
- **Rust** with cargo
- **Python 3.9+** with pip

### One-Command Setup

```bash
./run_comparison.sh build
```

This will:
- Compile OCaml benchmarks with dune
- Build Java uber-JAR with Maven
- Build Rust benchmarks in release mode (both variants)
- Set up Python virtual environments (both variants)

### Manual Setup

#### OCaml
```bash
cd ..
dune build bench/cross_language_bench.exe
```

#### Java
```bash
cd java
mvn clean package
```

#### Rust (apache-avro)
```bash
cd rust
cargo build --release
```

#### Rust (serde_avro_fast)
```bash
cd rust-fast
cargo build --release
```

#### Python (fastavro)
```bash
cd python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Python (official avro)
```bash
cd python-avro
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Running Benchmarks

### Basic Usage

```bash
./run_comparison.sh [OPTIONS] COMMAND
```

### Commands

- `build` - Build all benchmark programs
- `run` - Run single comparison benchmark
- `run-all` - Run all operations (encode, decode, container)
- `clean` - Clean build artifacts

### Options

- `-o, --operation OPERATION` - Benchmark operation: `encode`, `decode`, or `container` (default: encode)
- `-c, --count COUNT` - Number of records to process (default: 10000)
- `-w, --warmup WARMUP` - Number of warmup runs (default: 3)
- `-r, --runs RUNS` - Number of benchmark runs (default: 10)
- `-z, --compression CODEC` - Compression codec: `null` or `deflate` (default: null)

### Benchmark Operations

The script supports three types of benchmarks:

1. **`encode`** - Measures serialization performance
   - Creates Person records with varying data
   - Encodes to binary Avro format
   - Reports throughput in MB/s

2. **`decode`** - Measures deserialization performance
   - Pre-encodes Person records
   - Decodes from binary Avro format
   - Reports throughput in MB/s
   - **See [DECODE_BENCHMARKS.md](DECODE_BENCHMARKS.md) for detailed examples**

3. **`container`** - Measures container file I/O performance
   - Writes records to Avro container file with compression
   - Reads all records back
   - Reports write/read times and file size
- `-h, --help` - Show help message

### Examples

```bash
# Standard encoding benchmark with 10k records
./run_comparison.sh run

# Decoding benchmark with 100k records
./run_comparison.sh -o decode -c 100000 run

# Run all operations (encode, decode, container) at once
./run_comparison.sh run-all

# Container file benchmark with deflate compression
./run_comparison.sh -o container -z deflate run

# Quick decode test with fewer runs
./run_comparison.sh -o decode -c 1000 -w 2 -r 3 run

# Comprehensive benchmark suite with multiple sizes
./run_comparison.sh -c 1000 run-all
./run_comparison.sh -c 10000 run-all
./run_comparison.sh -c 100000 run-all

# Clean and rebuild
./run_comparison.sh clean
./run_comparison.sh build
```

### Interpreting Results

Results are saved in `results/` as:
- `*.md` - Markdown tables with timing statistics
- `*.json` - Raw JSON data for analysis

Example output:
```
| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `OCaml` | 7.6 ± 0.3 | 7.3 | 7.9 | 1.00 |
| `Rust (serde_avro_fast)` | 5.0 ± 0.5 | 4.5 | 5.6 | 0.66 ± 0.07 |
| `Rust (apache-avro)` | 15.7 ± 0.0 | 15.6 | 15.7 | 2.07 ± 0.08 |
...
```

---

## Performance Results

### Encoding Performance (10,000 records, ~496KB total)

**Direct Execution (Pure encoding time):**

| Implementation             | Time   | Throughput | vs avro-simple      |
|----------------------------|--------|------------|---------------------|
| **OCaml (ocaml-avro)**     | 1.6ms  | 318 MB/s   | 1.09x faster        |
| **Rust (serde_avro_fast)** | 1.6ms  | 310 MB/s   | 1.06x faster        |
| **OCaml (avro-simple)**    | 1.7ms  | 291 MB/s   | **1.0x** (baseline) |
| **Java (warmed up)**       | 6ms    | 83 MB/s    | 3.5x slower         |
| **Rust (apache-avro)**     | 12.8ms | 39 MB/s    | 7.5x slower         |
| **Python (fastavro)**      | 80ms   | 6 MB/s     | 47x slower          |
| **Python (avro)**          | ~350ms | 1.4 MB/s   | 206x slower         |

**Hyperfine Comparison (Includes process overhead):**

| Implementation             | Mean Time | Notes                            |
|----------------------------|-----------|----------------------------------|
| **Rust (serde_avro_fast)** | 5.0ms     | Lowest startup overhead          |
| **OCaml**                  | 7.6ms     | Fast native startup              |
| **Rust (apache-avro)**     | 15.7ms    | Value overhead + startup         |
| **Python (fastavro)**      | 68-80ms   | Python interpreter startup       |
| **Java (warmed up)**       | 263ms     | Includes JVM startup + warmup    |
| **Java (coldstart)**       | 280ms     | JVM startup without optimization |
| **Python (avro)**          | ~400ms    | Pure Python + interpreter        |

### Key Observations

1. **Both OCaml approaches are competitive** - avro-simple (runtime) and ocaml-avro (codegen) both deliver excellent performance

2. **Code generation vs runtime codec** - The ~9% difference shows OCaml's runtime codec design is highly efficient, unlike Java where SpecificRecord is 10-14x faster than GenericRecord

3. **Rust (serde_avro_fast) comparable** - Optimized Rust matches ocaml-avro performance

4. **Java competitive after warmup** - Pure encoding (6ms) is only 3.5x slower than OCaml, but 250ms JVM startup makes it poor for CLI tools

5. **Rust (apache-avro) has Value overhead** - 8x slower than serde_avro_fast due to intermediate representation

6. **Python (fastavro) usable** - C extensions make it acceptable for Python ecosystems

7. **Python (avro) very slow** - Pure Python implementation is 200x slower than OCaml

---

## Implementation Notes

### Design Approaches

| Language | Library         | Approach           | Trade-off                               |
|----------|-----------------|--------------------|-----------------------------------------|
| OCaml    | ocaml-avro      | Code generation    | 9% faster, no containers, compile-time  |
| OCaml    | avro-simple     | Direct codec       | Full features, runtime flexibility      |
| Rust     | serde_avro_fast | Direct serde       | No container files                      |
| Rust     | apache-avro     | Value intermediate | 8x slower but full features             |
| Java     | GenericRecord   | HashMap-based      | Flexible but slower than SpecificRecord |
| Python   | fastavro        | C extensions       | Fast for Python                         |
| Python   | avro            | Pure Python        | Very slow but portable                  |

---

## Contributing Improvements

**Have you found a faster approach or spotted an inefficiency?**

We welcome contributions that improve the fairness and accuracy of these benchmarks!

We aim to show:
1. **Real-world performance** - What users would actually experience
2. **Fair comparisons** - Apples-to-apples across languages
3. **Realistic code** - Following each language's best practices
4. **Transparency** - Clear documentation of trade-offs

If you believe any benchmark is unfair or unrepresentative, please open an issue!
