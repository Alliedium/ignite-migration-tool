package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class SelectAtomicsAction implements TransformAction<TransformAtomicsOutput> {
    private final PatchContext context;
    private final String from;

    private SelectAtomicsAction(Builder builder) {
        context = builder.context;
        from = builder.from;
    }

    @Override
    public TransformAtomicsOutput execute() {
        PathCombine rootPath = new PathCombine(Paths.get(from));
        Path atomicStructureFile = rootPath.plus(AvroFileNames.ATOMIC_STRUCTURE_FILE_NAME).getPath();
        if (!Files.exists(atomicStructureFile)) {
            throw new RuntimeException("no atomics schema found");
        }
        Schema schema;
        try {
            schema = (new Schema.Parser()).parse(atomicStructureFile.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Path dataFilePath = rootPath.plus(AvroFileNames.ATOMIC_DATA_FILE_NAME).getPath();

        PCollection<GenericRecord> records = context.getPipeline().apply(
                AvroIO.readGenericRecords(schema)
                .withBeamSchemas(true)
                .from(dataFilePath.toString()));
        PCollection<Row> rows = records.apply(ParDo.of(new AvroGenericToBeamRowsConverter(schema)))
                .setCoder(RowCoder.of(AvroUtils.toBeamSchema(schema)));

        return new TransformAtomicsOutput(rows, schema);
    }

    public static class Builder {
        private PatchContext context;
        private String from;

        public Builder context(PatchContext context) {
            this.context = context;
            return this;
        }

        public Builder from(String from) {
            this.from = from;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(context, "Patch context is not set");
            Objects.requireNonNull(from, "from is not set");
        }

        public SelectAtomicsAction build() {
            validate();
            return new SelectAtomicsAction(this);
        }
    }
}
