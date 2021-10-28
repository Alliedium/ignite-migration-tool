package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;
import org.alliedium.ignite.migration.util.UniqueKey;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.UUID;

class NestedAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {

    private final List<ICacheFieldMeta> fieldMetaList;

    public NestedAvroSchemaFieldAssembler(List<ICacheFieldMeta> fieldMetaList) {
        this.fieldMetaList = fieldMetaList;
    }

    @Override
    public void assembleAvroSchemaField(SchemaBuilder.FieldAssembler<Schema> fieldAssembler, ICacheFieldMeta fieldMeta) {
        final SchemaBuilder.FieldAssembler<Schema> nestedFieldAssembler = SchemaBuilder
                .record(generateUniqueRecordName(fieldMeta.getFieldType()))
                .fields();

        fieldMetaList.forEach(nestedFieldMeta -> {
            nestedFieldMeta.getAvroSchemaFieldAssembler().assembleAvroSchemaField(nestedFieldAssembler, nestedFieldMeta);
        });

        Schema nestedSchema = nestedFieldAssembler.endRecord();
        fieldAssembler.name(fieldMeta.getName()).type(nestedSchema).noDefault();
    }

    private String generateUniqueRecordName(String fieldType) {
        return UniqueKey.generateWithRecordType(fieldType);
    }
}
