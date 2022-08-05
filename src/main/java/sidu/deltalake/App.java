package sidu.deltalake;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Hello world!
 * https://www.irony.dev/posts/azure-parquet/
 */
public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        int fileCount = 3;
        if (args != null && args.length > 0) {
            fileCount = Integer.parseInt(args[0]);
        }

        new App().run(fileCount);
    }

    public void run(int fileCount) throws IOException {
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig();
        Schema schema = SchemaBuilder
                .builder()
                .record("contacts")
                .fields()
                .requiredInt("id")
                .requiredString("name")
                .endRecord();

        for (int i = 0; i < fileCount; ++i) {
            GenericRecord record = new GenericRecordBuilder(schema).set("id", i).set("name", UUID.randomUUID().toString()).build();
            List<GenericRecord> list = new ArrayList<>();
            list.add(record);
            String fileName = String.format("%s/T3-%s.snappy.parquet", deltaLakeConfig.getDeltaTablePath(), i);
            writeBulk(fileName, schema, list, deltaLakeConfig.getHadoopConf());
        }
    }

    private void writeBulk(String path, Schema schema, Collection<GenericRecord> records, Configuration hadoopConf) throws IOException {
        try (ParquetWriter<GenericRecord> parquetWriter =
                     AvroParquetWriter.<GenericRecord>builder(new Path(path))
                             .withConf(hadoopConf)
                             .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .withSchema(schema)
                             .build()) {

            for (GenericRecord record : records) {
                parquetWriter.write(record);
            }

            LOG.info("Write file {} done.", path);
        }
    }
}
