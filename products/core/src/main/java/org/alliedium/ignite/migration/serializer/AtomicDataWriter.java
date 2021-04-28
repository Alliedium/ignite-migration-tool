package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.IDataWriter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Map;

public class AtomicDataWriter implements IDataWriter<Map.Entry<String, Long>> {

    private static final String AVRO_IGNITE_ATOMIC_LONG_NAME_FIELD_NAME = "igniteAtomicLongName";
    private static final String AVRO_IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME = "igniteAtomicLongValue";

    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final Schema schema;

    public AtomicDataWriter(DataFileWriter<GenericRecord> dataFileWriter, Schema schema) {
        this.dataFileWriter = dataFileWriter;
        this.schema = schema;
    }

    @Override
    public void write(Map.Entry<String, Long> entry) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(AVRO_IGNITE_ATOMIC_LONG_NAME_FIELD_NAME, entry.getKey());
        genericRecord.put(AVRO_IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME, entry.getValue());
        try {
            dataFileWriter.append(genericRecord);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write atomic long to file", e);
        }
    }

    @Override
    public void close() throws Exception {
        dataFileWriter.close();
    }
}
