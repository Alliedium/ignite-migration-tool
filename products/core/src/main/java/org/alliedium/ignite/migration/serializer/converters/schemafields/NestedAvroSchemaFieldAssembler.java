package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;
import org.alliedium.ignite.migration.util.TypeUtils;
import org.alliedium.ignite.migration.util.UniqueKey;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;

class NestedAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {

    private final List<ICacheFieldMeta> fieldMetaList;

    public NestedAvroSchemaFieldAssembler(List<ICacheFieldMeta> fieldMetaList) {
        this.fieldMetaList = fieldMetaList;
    }

    @Override
    public void assembleAvroSchemaField(SchemaBuilder.FieldAssembler<Schema> fieldAssembler, ICacheFieldMeta fieldMeta) {
        final SchemaBuilder.FieldAssembler<Schema> nestedFieldAssembler = SchemaBuilder
                .record(UniqueKey.generate())
                .fields();

        fieldMetaList.forEach(nestedFieldMeta -> {
            nestedFieldMeta.getAvroSchemaFieldAssembler().assembleAvroSchemaField(nestedFieldAssembler, nestedFieldMeta);
        });

        Schema nestedSchema = nestedFieldAssembler.endRecord();
        nestedSchema.addProp(TypeUtils.FIELD_TYPE, fieldMeta.getFieldType());
        fieldAssembler.name(fieldMeta.getName()).type(nestedSchema).noDefault();
    }
}
