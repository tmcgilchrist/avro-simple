// Benchmark using serde_avro_fast crate (high-performance alternative)
// This uses direct serde integration without intermediate Value representation
// Claims 10-20x faster than apache-avro

use serde::{Deserialize, Serialize};
use std::env;
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

fn get_schema() -> serde_avro_fast::Schema {
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
    schema_str.parse().unwrap()
}

fn benchmark_encode(count: i32) {
    let schema = get_schema();
    let people: Vec<Person> = (0..count).map(create_person).collect();

    let start = Instant::now();
    let mut total_bytes = 0;

    // Use SerializerConfig once for all records
    let mut config = serde_avro_fast::ser::SerializerConfig::new(&schema);

    for person in &people {
        // Direct encoding without intermediate Value
        let bytes = serde_avro_fast::to_datum(person, Vec::new(), &mut config).unwrap();
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
    let mut config = serde_avro_fast::ser::SerializerConfig::new(&schema);
    let mut encoded = Vec::new();
    for person in &people {
        let bytes = serde_avro_fast::to_datum(person, Vec::new(), &mut config).unwrap();
        encoded.push(bytes);
    }

    let total_bytes: usize = encoded.iter().map(|b| b.len()).sum();

    // Benchmark decode
    let start = Instant::now();
    for bytes in &encoded {
        let _person: Person = serde_avro_fast::from_datum_slice(bytes, &schema).unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mb_per_sec = (total_bytes as f64 / elapsed) / 1_000_000.0;

    println!(
        "Decoded {} records in {:.6} seconds ({:.2} MB/s, {} bytes)",
        count, elapsed, mb_per_sec, total_bytes
    );
}

fn benchmark_container(_count: i32, _compression: &str) {
    // serde_avro_fast doesn't support container files with headers/compression
    // It's optimized for raw encoding/decoding only
    eprintln!("Container file benchmarks not supported by serde_avro_fast");
    eprintln!("This crate is optimized for raw encoding/decoding without container format");
    std::process::exit(1);
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
