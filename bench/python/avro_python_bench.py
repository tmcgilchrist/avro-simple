#!/usr/bin/env python3
"""
Cross-language Avro benchmark - Python implementation

This benchmark uses fastavro for best performance comparison.
Falls back to the official avro library if fastavro is not available.
"""

import sys
import time
import io
import os
import tempfile

try:
    import fastavro
    USE_FASTAVRO = True
except ImportError:
    import avro.schema
    import avro.io
    import avro.datafile
    USE_FASTAVRO = False

SCHEMA = {
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "email", "type": ["null", "string"]},
        {"name": "phone_numbers", "type": {"type": "array", "items": "string"}},
    ],
}


def create_person(i):
    return {
        "name": f"Person_{i}",
        "age": 20 + (i % 60),
        "email": f"person{i}@example.com" if i % 3 == 0 else None,
        "phone_numbers": [f"+1-555-{i * 10 + j:04d}" for j in range(1 + i % 3)],
    }


def benchmark_encode_fastavro(count):
    people = [create_person(i) for i in range(count)]

    start_time = time.time()
    encoded = []
    for person in people:
        output = io.BytesIO()
        fastavro.schemaless_writer(output, SCHEMA, person)
        encoded.append(output.getvalue())

    elapsed = time.time() - start_time
    total_bytes = sum(len(e) for e in encoded)
    mb_per_sec = (total_bytes / elapsed) / 1_000_000

    print(f"Encoded {count} records in {elapsed:.6f} seconds ({mb_per_sec:.2f} MB/s, {total_bytes} bytes)")


def benchmark_encode_avro(count):
    people = [create_person(i) for i in range(count)]
    schema = avro.schema.parse(str(SCHEMA).replace("'", '"'))

    start_time = time.time()
    encoded = []
    for person in people:
        output = io.BytesIO()
        writer = avro.io.DatumWriter(schema)
        encoder = avro.io.BinaryEncoder(output)
        writer.write(person, encoder)
        encoded.append(output.getvalue())

    elapsed = time.time() - start_time
    total_bytes = sum(len(e) for e in encoded)
    mb_per_sec = (total_bytes / elapsed) / 1_000_000

    print(f"Encoded {count} records in {elapsed:.6f} seconds ({mb_per_sec:.2f} MB/s, {total_bytes} bytes)")


def benchmark_decode_fastavro(count):
    people = [create_person(i) for i in range(count)]
    encoded = []
    for person in people:
        output = io.BytesIO()
        fastavro.schemaless_writer(output, SCHEMA, person)
        encoded.append(output.getvalue())

    start_time = time.time()
    for data in encoded:
        fastavro.schemaless_reader(io.BytesIO(data), SCHEMA)

    elapsed = time.time() - start_time
    total_bytes = sum(len(e) for e in encoded)
    mb_per_sec = (total_bytes / elapsed) / 1_000_000

    print(f"Decoded {count} records in {elapsed:.6f} seconds ({mb_per_sec:.2f} MB/s, {total_bytes} bytes)")


def benchmark_decode_avro(count):
    people = [create_person(i) for i in range(count)]
    schema = avro.schema.parse(str(SCHEMA).replace("'", '"'))

    encoded = []
    writer = avro.io.DatumWriter(schema)
    for person in people:
        output = io.BytesIO()
        encoder = avro.io.BinaryEncoder(output)
        writer.write(person, encoder)
        encoded.append(output.getvalue())

    start_time = time.time()
    reader = avro.io.DatumReader(schema)
    for data in encoded:
        decoder = avro.io.BinaryDecoder(io.BytesIO(data))
        reader.read(decoder)

    elapsed = time.time() - start_time
    total_bytes = sum(len(e) for e in encoded)
    mb_per_sec = (total_bytes / elapsed) / 1_000_000

    print(f"Decoded {count} records in {elapsed:.6f} seconds ({mb_per_sec:.2f} MB/s, {total_bytes} bytes)")


def benchmark_container_fastavro(count, compression):
    people = [create_person(i) for i in range(count)]

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.avro', delete=False) as f:
        temp_path = f.name

    try:
        # Write
        start_write = time.time()
        with open(temp_path, 'wb') as out:
            fastavro.writer(out, SCHEMA, people, codec=compression)
        elapsed_write = time.time() - start_write

        # Read
        start_read = time.time()
        count_read = 0
        with open(temp_path, 'rb') as fo:
            for _ in fastavro.reader(fo):
                count_read += 1
        elapsed_read = time.time() - start_read

        file_size = os.path.getsize(temp_path)

        print(f"Container[{compression}]: Wrote {count} records in {elapsed_write:.6f} seconds, "
              f"Read in {elapsed_read:.6f} seconds ({file_size} bytes)")
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def benchmark_container_avro(count, compression):
    people = [create_person(i) for i in range(count)]
    schema = avro.schema.parse(str(SCHEMA).replace("'", '"'))

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.avro', delete=False) as f:
        temp_path = f.name

    try:
        # Write
        start_write = time.time()
        with open(temp_path, 'wb') as out:
            writer = avro.datafile.DataFileWriter(out, avro.io.DatumWriter(), schema, codec=compression)
            for person in people:
                writer.append(person)
            writer.close()
        elapsed_write = time.time() - start_write

        # Read
        start_read = time.time()
        count_read = 0
        with open(temp_path, 'rb') as fo:
            reader = avro.datafile.DataFileReader(fo, avro.io.DatumReader())
            for _ in reader:
                count_read += 1
            reader.close()
        elapsed_read = time.time() - start_read

        file_size = os.path.getsize(temp_path)

        print(f"Container[{compression}]: Wrote {count} records in {elapsed_write:.6f} seconds, "
              f"Read in {elapsed_read:.6f} seconds ({file_size} bytes)")
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def main():
    operation = sys.argv[1] if len(sys.argv) > 1 else "encode"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    compression = sys.argv[3] if len(sys.argv) > 3 else "null"

    if USE_FASTAVRO:
        if operation == "encode":
            benchmark_encode_fastavro(count)
        elif operation == "decode":
            benchmark_decode_fastavro(count)
        elif operation == "container":
            benchmark_container_fastavro(count, compression)
        else:
            print(f"Usage: {sys.argv[0]} [encode|decode|container] [count] [compression]", file=sys.stderr)
            sys.exit(1)
    else:
        if operation == "encode":
            benchmark_encode_avro(count)
        elif operation == "decode":
            benchmark_decode_avro(count)
        elif operation == "container":
            benchmark_container_avro(count, compression)
        else:
            print(f"Usage: {sys.argv[0]} [encode|decode|container] [count] [compression]", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
