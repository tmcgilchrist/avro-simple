// Benchmark using apache-avro crate (official Apache implementation)
// This uses the standard Value-based approach which has inherent overhead
// See PERFORMANCE_ANALYSIS.md for details

use apache_avro::{from_value, to_value, Codec, Reader, Schema, Writer};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    age: i32,
    email: Option<String>,
    phone_numbers: Vec<String>,
}

fn create_person(i: i32) -> Person {
    Person {
        name: format!("Person_{}", i),
        age: 20 + (i % 60),
        email: if i % 3 == 0 {
            Some(format!("person{}@example.com", i))
        } else {
            None
        },
        phone_numbers: (0..(1 + i % 3))
            .map(|j| format!("+1-555-{:04}", i * 10 + j))
            .collect(),
    }
}

fn get_schema() -> Schema {
    let schema_str = r#"
    {
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "email", "type": ["null", "string"]},
            {"name": "phone_numbers", "type": {"type": "array", "items": "string"}}
        ]
    }
    "#;
    Schema::parse_str(schema_str).unwrap()
}

fn benchmark_encode(count: i32) {
    let schema = get_schema();
    let people: Vec<Person> = (0..count).map(create_person).collect();

    let start = Instant::now();
    let mut total_bytes = 0;

    // NOTE: apache-avro crate has inherent performance limitations:
    // 1. to_value() creates intermediate Value representation (serde overhead)
    // 2. to_avro_datum() then encodes Value to bytes
    // This two-step process is ~10-20x slower than direct encoding
    // Alternative crates like serde_avro_fast claim 10-20x speedup by avoiding Value

    for person in &people {
        // Convert to Value, then encode (matches apache-avro idiom)
        let value = to_value(person).unwrap();
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        total_bytes += bytes.len();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mb_per_sec = (total_bytes as f64 / elapsed) / 1_000_000.0;

    println!(
        "Encoded {} records in {:.6} seconds ({:.2} MB/s, {} bytes)",
        count, elapsed, mb_per_sec, total_bytes
    );
}

fn benchmark_decode(count: i32) {
    let schema = get_schema();
    let people: Vec<Person> = (0..count).map(create_person).collect();

    // Encode first
    let mut encoded = Vec::new();
    for person in &people {
        let value = to_value(person).unwrap();
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        encoded.push(bytes);
    }

    let total_bytes: usize = encoded.iter().map(|b| b.len()).sum();

    // Benchmark decode
    let start = Instant::now();
    for bytes in &encoded {
        let value = apache_avro::from_avro_datum(&schema, &mut &bytes[..], None).unwrap();
        let _person: Person = from_value(&value).unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mb_per_sec = (total_bytes as f64 / elapsed) / 1_000_000.0;

    println!(
        "Decoded {} records in {:.6} seconds ({:.2} MB/s, {} bytes)",
        count, elapsed, mb_per_sec, total_bytes
    );
}

fn benchmark_container(count: i32, compression: &str) {
    let schema = get_schema();
    let people: Vec<Person> = (0..count).map(create_person).collect();

    let codec = match compression {
        "null" => Codec::Null,
        "deflate" => Codec::Deflate(Default::default()),
        _ => Codec::Null,
    };

    let temp_path = format!("/tmp/bench_{}.avro", compression);

    // Write
    let start_write = Instant::now();
    {
        let mut writer = Writer::with_codec(&schema, File::create(&temp_path).unwrap(), codec);
        for person in &people {
            writer.append_ser(person).unwrap();
        }
        writer.flush().unwrap();
    }
    let elapsed_write = start_write.elapsed().as_secs_f64();

    // Read
    let start_read = Instant::now();
    let reader = Reader::new(File::open(&temp_path).unwrap()).unwrap();
    let mut _count_read = 0;
    for _record in reader {
        _count_read += 1;
    }
    let elapsed_read = start_read.elapsed().as_secs_f64();

    let metadata = std::fs::metadata(&temp_path).unwrap();
    let file_size = metadata.len();

    std::fs::remove_file(&temp_path).unwrap();

    println!(
        "Container[{}]: Wrote {} records in {:.6} seconds, Read in {:.6} seconds ({} bytes)",
        compression, count, elapsed_write, elapsed_read, file_size
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let operation = args.get(1).map(String::as_str).unwrap_or("encode");
    let count: i32 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);
    let compression = args.get(3).map(String::as_str).unwrap_or("null");

    match operation {
        "encode" => benchmark_encode(count),
        "decode" => benchmark_decode(count),
        "container" => benchmark_container(count, compression),
        _ => {
            eprintln!(
                "Usage: {} [encode|decode|container] [count] [compression]",
                args[0]
            );
            std::process::exit(1);
        }
    }
}
