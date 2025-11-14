import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.*;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.CodecFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class AvroJavaBench {
    private static final String SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Person\","
        + "\"fields\":["
        + "  {\"name\":\"name\",\"type\":\"string\"},"
        + "  {\"name\":\"age\",\"type\":\"int\"},"
        + "  {\"name\":\"email\",\"type\":[\"null\",\"string\"]},"
        + "  {\"name\":\"phone_numbers\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
        + "]}";

    private static Schema schema;

    static {
        try {
            schema = new Schema.Parser().parse(SCHEMA_JSON);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static GenericRecord createPerson(int i) {
        GenericRecord person = new GenericData.Record(schema);
        person.put("name", "Person_" + i);
        person.put("age", 20 + (i % 60));
        person.put("email", (i % 3 == 0) ? "person" + i + "@example.com" : null);

        List<String> phones = new ArrayList<>();
        for (int j = 0; j < (1 + i % 3); j++) {
            phones.add(String.format("+1-555-%04d", i * 10 + j));
        }
        person.put("phone_numbers", phones);

        return person;
    }

    private static void benchmarkEncode(int count) throws IOException {
        List<GenericRecord> people = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            people.add(createPerson(i));
        }

        long startTime = System.nanoTime();
        int totalBytes = 0;
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        // Reuse encoder and output stream for better performance
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = null;

        for (GenericRecord person : people) {
            out.reset();
            encoder = EncoderFactory.get().binaryEncoder(out, encoder);
            writer.write(person, encoder);
            encoder.flush();
            totalBytes += out.size();
        }

        double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;
        double mbPerSec = (totalBytes / elapsed) / 1_000_000.0;

        System.out.printf("Encoded %d records in %.6f seconds (%.2f MB/s, %d bytes)%n",
                count, elapsed, mbPerSec, totalBytes);
    }

    private static void benchmarkDecode(int count) throws IOException {
        List<byte[]> encoded = new ArrayList<>(count);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        // Encode first (with reuse for efficiency)
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = null;
        for (int i = 0; i < count; i++) {
            out.reset();
            encoder = EncoderFactory.get().binaryEncoder(out, encoder);
            writer.write(createPerson(i), encoder);
            encoder.flush();
            encoded.add(out.toByteArray());
        }

        // Benchmark decode
        long startTime = System.nanoTime();
        int totalBytes = 0;
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = null;

        for (byte[] data : encoded) {
            decoder = DecoderFactory.get().binaryDecoder(data, decoder);
            reader.read(null, decoder);
            totalBytes += data.length;
        }

        double elapsed = (System.nanoTime() - startTime) / 1_000_000_000.0;
        double mbPerSec = (totalBytes / elapsed) / 1_000_000.0;

        System.out.printf("Decoded %d records in %.6f seconds (%.2f MB/s, %d bytes)%n",
                count, elapsed, mbPerSec, totalBytes);
    }

    private static void benchmarkContainer(int count, String compression) throws IOException {
        File tempFile = File.createTempFile("bench_" + compression, ".avro");
        tempFile.deleteOnExit();

        List<GenericRecord> people = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            people.add(createPerson(i));
        }

        // Write
        long startWrite = System.nanoTime();
        CodecFactory codec = compression.equals("deflate") ? CodecFactory.deflateCodec(6) : CodecFactory.nullCodec();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.setCodec(codec);
            writer.create(schema, tempFile);
            for (GenericRecord person : people) {
                writer.append(person);
            }
        }
        double elapsedWrite = (System.nanoTime() - startWrite) / 1_000_000_000.0;

        // Read
        long startRead = System.nanoTime();
        int countRead = 0;
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(tempFile, new GenericDatumReader<>(schema))) {
            while (reader.hasNext()) {
                reader.next();
                countRead++;
            }
        }
        double elapsedRead = (System.nanoTime() - startRead) / 1_000_000_000.0;

        long fileSize = tempFile.length();
        tempFile.delete();

        System.out.printf("Container[%s]: Wrote %d records in %.6f seconds, Read in %.6f seconds (%d bytes)%n",
                compression, count, elapsedWrite, elapsedRead, fileSize);
    }

    private static void warmupJVM(String operation, int count, String compression) throws IOException {
        // Run multiple warmup iterations to trigger JIT compilation
        // Suppress output during warmup
        java.io.PrintStream originalOut = System.out;
        System.setOut(new java.io.PrintStream(new java.io.OutputStream() {
            public void write(int b) {}
        }));

        int warmupIterations = 10;
        for (int i = 0; i < warmupIterations; i++) {
            switch (operation) {
                case "encode":
                    benchmarkEncode(Math.min(count, 1000));
                    break;
                case "decode":
                    benchmarkDecode(Math.min(count, 1000));
                    break;
                case "container":
                    benchmarkContainer(Math.min(count, 1000), compression);
                    break;
            }
        }

        // Restore output
        System.setOut(originalOut);
    }

    public static void main(String[] args) throws IOException {
        String operation = args.length > 0 ? args[0] : "encode";
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 10000;
        String compression = args.length > 2 ? args[2] : "null";
        boolean warmup = args.length > 3 && args[3].equals("--warmup");

        if (warmup) {
            // Warmup the JVM before benchmarking
            warmupJVM(operation, count, compression);
        }

        switch (operation) {
            case "encode":
                benchmarkEncode(count);
                break;
            case "decode":
                benchmarkDecode(count);
                break;
            case "container":
                benchmarkContainer(count, compression);
                break;
            default:
                System.err.println("Usage: java AvroJavaBench [encode|decode|container] [count] [compression] [--warmup]");
                System.exit(1);
        }
    }
}
