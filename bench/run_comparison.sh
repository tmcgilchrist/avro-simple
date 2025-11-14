#!/usr/bin/env bash
#
# Cross-language Avro Benchmark Comparison
#
# This script compares the performance of Avro implementations across
# OCaml, Java, Rust, and Python using hyperfine.
#
# Requirements:
# - hyperfine (https://github.com/sharkdp/hyperfine)
# - OCaml with dune
# - Java 11+ with Maven
# - Rust with cargo
# - Python 3.9+ with pip

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BENCH_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default parameters
COUNT="${COUNT:-10000}"
WARMUP="${WARMUP:-3}"
RUNS="${RUNS:-10}"
OPERATION="${OPERATION:-encode}"
COMPRESSION="${COMPRESSION:-null}"

print_header() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        print_error "$1 is not installed. Please install it first."
        return 1
    fi
    return 0
}

build_ocaml() {
    print_info "Building OCaml benchmark..."
    cd "$BENCH_DIR/.."
    dune build bench/cross_language_bench.exe
    cd "$BENCH_DIR"
}

build_java() {
    print_info "Building Java benchmark..."
    cd java
    if [ ! -f "pom.xml" ]; then
        print_error "pom.xml not found in java/"
        return 1
    fi
    mvn clean package -q || {
        print_error "Maven build failed"
        return 1
    }
    cd "$BENCH_DIR"
}

build_rust() {
    print_info "Building Rust benchmarks (apache-avro)..."
    cd rust
    if [ ! -f "Cargo.toml" ]; then
        print_error "Cargo.toml not found in rust/"
        return 1
    fi
    cargo build --release --quiet || {
        print_error "Cargo build failed"
        return 1
    }
    cd "$BENCH_DIR"
}

build_rust_fast() {
    print_info "Building Rust benchmarks (serde_avro_fast)..."
    cd rust-fast
    if [ ! -f "Cargo.toml" ]; then
        print_error "Cargo.toml not found in rust-fast/"
        return 1
    fi
    cargo build --release --quiet || {
        print_error "Cargo build failed"
        return 1
    }
    cd "$BENCH_DIR"
}

setup_python() {
    print_info "Setting up Python benchmark (fastavro)..."
    if [ ! -f "python/requirements.txt" ]; then
        print_error "requirements.txt not found in python/"
        return 1
    fi

    # Check if virtual environment exists, create if not
    if [ ! -d "python/venv" ]; then
        python3 -m venv python/venv
    fi

    # shellcheck disable=SC1091
    source python/venv/bin/activate
    pip install -q -r python/requirements.txt
    deactivate
}

setup_python_avro() {
    print_info "Setting up Python benchmark (official avro)..."
    if [ ! -f "python-avro/requirements.txt" ]; then
        print_error "requirements.txt not found in python-avro/"
        return 1
    fi

    # Check if virtual environment exists, create if not
    if [ ! -d "python-avro/venv" ]; then
        python3 -m venv python-avro/venv
    fi

    # shellcheck disable=SC1091
    source python-avro/venv/bin/activate
    pip install -q -r python-avro/requirements.txt
    deactivate
}

run_benchmark() {
    local name=$1
    local command=$2

    print_info "Running $name benchmark..."

    if ! check_command hyperfine; then
        print_error "hyperfine not found. Install with: brew install hyperfine (macOS) or cargo install hyperfine"
        return 1
    fi

    hyperfine \
        --warmup "$WARMUP" \
        --runs "$RUNS" \
        --export-markdown "results/${OPERATION}_${COUNT}_comparison.md" \
        --export-json "results/${OPERATION}_${COUNT}_comparison.json" \
        "$command"
}

run_full_comparison() {
    local op=$1
    local cnt=$2
    local comp=$3

    print_header "Benchmarking: $op operation with $cnt records (compression: $comp)"

    mkdir -p results

    # Build commands
    local ocaml_cmd="../_build/default/bench/cross_language_bench.exe $op $cnt $comp"
    local java_coldstart_cmd="java -jar java/target/avro-java-bench-1.0-SNAPSHOT.jar $op $cnt $comp"
    local java_warmup_cmd="java -jar java/target/avro-java-bench-1.0-SNAPSHOT.jar $op $cnt $comp --warmup"
    local rust_apache_cmd="rust/target/release/avro-rust-bench $op $cnt $comp"
    local rust_fast_cmd="rust-fast/target/release/avro-rust-bench-fast $op $cnt $comp"
    local python_fastavro_cmd="python/venv/bin/python python/avro_python_bench.py $op $cnt $comp"
    local python_avro_cmd="python-avro/venv/bin/python python-avro/avro_python_bench.py $op $cnt $comp"

    # Run hyperfine comparison
    # Note: serde_avro_fast doesn't support container operations
    if [ "$op" = "container" ]; then
        hyperfine \
            --warmup "$WARMUP" \
            --runs "$RUNS" \
            --export-markdown "results/${op}_${cnt}_${comp}_comparison.md" \
            --export-json "results/${op}_${cnt}_${comp}_comparison.json" \
            --command-name "OCaml" "$ocaml_cmd" \
            --command-name "Java (coldstart)" "$java_coldstart_cmd" \
            --command-name "Java (warmed up)" "$java_warmup_cmd" \
            --command-name "Rust (apache-avro)" "$rust_apache_cmd" \
            --command-name "Python (fastavro)" "$python_fastavro_cmd" \
            --command-name "Python (avro)" "$python_avro_cmd"
    else
        hyperfine \
            --warmup "$WARMUP" \
            --runs "$RUNS" \
            --export-markdown "results/${op}_${cnt}_${comp}_comparison.md" \
            --export-json "results/${op}_${cnt}_${comp}_comparison.json" \
            --command-name "OCaml" "$ocaml_cmd" \
            --command-name "Java (coldstart)" "$java_coldstart_cmd" \
            --command-name "Java (warmed up)" "$java_warmup_cmd" \
            --command-name "Rust (apache-avro)" "$rust_apache_cmd" \
            --command-name "Rust (serde_avro_fast)" "$rust_fast_cmd" \
            --command-name "Python (fastavro)" "$python_fastavro_cmd" \
            --command-name "Python (avro)" "$python_avro_cmd"
    fi

    print_info "Results saved to results/${op}_${cnt}_${comp}_comparison.{md,json}"
}

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] COMMAND

Cross-language Avro benchmark comparison using hyperfine.

COMMANDS:
    build           Build all benchmark programs
    run             Run comparison benchmark
    clean           Clean build artifacts

OPTIONS:
    -o, --operation OPERATION    Benchmark operation: encode, decode, or container (default: encode)
    -c, --count COUNT            Number of records to process (default: 10000)
    -w, --warmup WARMUP          Number of warmup runs (default: 3)
    -r, --runs RUNS              Number of benchmark runs (default: 10)
    -z, --compression CODEC      Compression codec: null or deflate (default: null)
    -h, --help                   Show this help message

EXAMPLES:
    # Build all benchmarks
    $0 build

    # Run encoding benchmark with 10k records
    $0 run

    # Run decoding benchmark with 50k records
    $0 -o decode -c 50000 run

    # Run container file benchmark with deflate compression
    $0 -o container -z deflate run

    # Run with custom warmup and run counts
    $0 -w 5 -r 20 run

ENVIRONMENT VARIABLES:
    COUNT          Number of records (overridden by -c)
    WARMUP         Number of warmup runs (overridden by -w)
    RUNS           Number of benchmark runs (overridden by -r)
    OPERATION      Benchmark operation (overridden by -o)
    COMPRESSION    Compression codec (overridden by -z)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--operation)
            OPERATION="$2"
            shift 2
            ;;
        -c|--count)
            COUNT="$2"
            shift 2
            ;;
        -w|--warmup)
            WARMUP="$2"
            shift 2
            ;;
        -r|--runs)
            RUNS="$2"
            shift 2
            ;;
        -z|--compression)
            COMPRESSION="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        build)
            COMMAND="build"
            shift
            ;;
        run)
            COMMAND="run"
            shift
            ;;
        clean)
            COMMAND="clean"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Execute command
case "${COMMAND:-}" in
    build)
        print_header "Building All Benchmarks"
        build_ocaml
        build_java
        build_rust
        build_rust_fast
        setup_python
        setup_python_avro
        print_info "All benchmarks built successfully!"
        ;;
    run)
        print_header "Running Benchmark Comparison"
        print_info "Operation: $OPERATION"
        print_info "Records: $COUNT"
        print_info "Compression: $COMPRESSION"
        print_info "Warmup runs: $WARMUP"
        print_info "Benchmark runs: $RUNS"
        echo
        run_full_comparison "$OPERATION" "$COUNT" "$COMPRESSION"
        ;;
    clean)
        print_header "Cleaning Build Artifacts"
        cd "$BENCH_DIR/.."
        dune clean
        cd "$BENCH_DIR"
        [ -d java/target ] && rm -rf java/target
        [ -d rust/target ] && rm -rf rust/target
        [ -d rust-fast/target ] && rm -rf rust-fast/target
        [ -d python/venv ] && rm -rf python/venv
        [ -d python-avro/venv ] && rm -rf python-avro/venv
        [ -d results ] && rm -rf results
        print_info "Cleanup complete!"
        ;;
    *)
        print_error "No command specified"
        show_usage
        exit 1
        ;;
esac
