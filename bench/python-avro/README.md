# Python Avro Benchmark (Official Apache Avro)

This directory contains the Python benchmark using the **official Apache Avro library** (pure Python implementation).

## About

The `avro` package is the official Apache Avro implementation for Python:
- Pure Python implementation
- Reference implementation from Apache
- Significantly slower than `fastavro` (C extensions)

**Comparison:**
- This benchmark (avro): ~300-500ms for 10k records
- fastavro benchmark: ~80ms for 10k records
- **6-8x slower** than fastavro

## Why Include This?

While production systems typically use `fastavro`, we include the official implementation to:
1. Show the official Apache reference performance
2. Provide a complete comparison across official implementations
3. Demonstrate the performance gap with optimized alternatives

## Building

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Running

```bash
# Activate virtual environment
source venv/bin/activate

# Encoding benchmark
python avro_python_bench.py encode 10000

# Decoding benchmark
python avro_python_bench.py decode 10000

# Container file benchmark
python avro_python_bench.py container 10000 null
python avro_python_bench.py container 10000 deflate
```

## Performance

**Expected performance (10,000 records):**
```
Encoded 10000 records in 0.350000 seconds (1.42 MB/s, 495844 bytes)
```

**Comparison:**
- **Official avro**: ~350ms (pure Python)
- **fastavro**: ~80ms (C extensions)
- **OCaml**: ~7ms (native code)
- **Rust (serde_avro_fast)**: ~5ms (native code)

The official library is significantly slower due to:
- Pure Python implementation
- No C extensions
- Overhead from dynamic typing
- GIL limitations

## See Also

- `../python/` - fastavro benchmark (optimized Python)
- [Official Apache Avro Python](https://avro.apache.org/docs/current/api/python/)
- [fastavro](https://fastavro.readthedocs.io/) - High-performance alternative
