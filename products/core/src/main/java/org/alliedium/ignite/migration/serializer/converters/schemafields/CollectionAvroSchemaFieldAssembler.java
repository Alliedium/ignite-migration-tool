package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;
import org.alliedium.ignite.migration.util.TypeUtils;
import org.alliedium.ignite.migration.util.UniqueKey;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

class CollectionAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {
    @Override
    public void assembleAvroSchemaField(SchemaBuilder.FieldAssembler<Schema> fieldAssembler, ICacheFieldMeta fieldMeta) {
        final SchemaBuilder.FieldAssembler<Schema> collectionElementsFieldAssembler = SchemaBuilder
                .record(UniqueKey.generate())
                .fields();

        fieldMeta.getNested().forEach((fieldName, nestedFieldMeta) -> {
            nestedFieldMeta.getAvroSchemaFieldAssembler().assembleAvroSchemaField(collectionElementsFieldAssembler, nestedFieldMeta);
        });

        Schema nestedSchema = collectionElementsFieldAssembler.endRecord();

        Schema collectionSchema = SchemaBuilder.array().items(nestedSchema.getField(TypeUtils.VALUE).schema());
        collectionSchema.addProp(TypeUtils.FIELD_TYPE, fieldMeta.getFieldType());

        fieldAssembler.name(fieldMeta.getName()).type(collectionSchema).noDefault();
    }
}
