package com.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.serializer.AvroFileWriter;
import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.nio.file.Paths;

public class AtomicsWriter {
    private final TransformAction<TransformAtomicsOutput> transformAction;

    public AtomicsWriter(TransformAction<TransformAtomicsOutput> transformAction) {
        this.transformAction = transformAction;
    }

    public void writeTo(String destinationDirectory) {
        TransformAtomicsOutput out = transformAction.execute();
        PCollection<Row> rows = out.getPCollection();
        Schema schema = out.getSchema();

        DoFn<Row, GenericRecord> convertToGenericRecords = new BeamRowsToAvroConverter(schema);

        rows.apply(ParDo.of(convertToGenericRecords))
                .setCoder(AvroCoder.of(schema))
                .apply(
                        "WriteToAvro",
                        AvroIO.writeGenericRecords(schema)
                                .to(destinationDirectory + AvroFileNames.ATOMIC_DATA_FILE_NAME)
                                .withoutSharding()
                );

        AvroFileWriter avroFileWriter = new AvroFileWriter();
        avroFileWriter.writeAvroSchemaToFile(schema,
                Paths.get(destinationDirectory + AvroFileNames.ATOMIC_STRUCTURE_FILE_NAME));
    }

}
