package io.github.alliedium.ignite.migration.patchtools;

import io.github.alliedium.ignite.migration.serializer.AvroCacheConfigurationsWriter;
import io.github.alliedium.ignite.migration.util.PathCombine;
import io.github.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import io.github.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import io.github.alliedium.ignite.migration.serializer.AvroFileWriter;
import io.github.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

public class CacheWriter {
    private static final IIgniteDTOConverter<String, Collection<QueryEntity>> queryEntityConverter =
            IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER;
    private static final IIgniteDTOConverter<String, CacheConfiguration<Object, BinaryObject>> cacheConfigConverter =
            IgniteObjectStringConverter.CACHE_CONFIG_CONVERTER;
    private final TransformAction<TransformOutput> transformAction;

    public CacheWriter(TransformAction<TransformOutput> transformAction) {
        this.transformAction = transformAction;
    }

    public void writeTo(String destinationCachePath) {
        TransformOutput result = transformAction.execute();

        Schema schema = result.getSchema();

        DoFn<Row, GenericRecord> convertToGenericRecords = new BeamRowsToAvroConverter(schema);

        result.getPCollection().apply(ParDo.of(convertToGenericRecords))
                .setCoder(AvroCoder.of(schema))
                .apply(
                        "WriteToAvro",
                        AvroIO.writeGenericRecords(schema)
                                .to(destinationCachePath + AvroFileNames.CACHE_DATA_FILENAME)
                                .withoutSharding()
                );

        AvroFileWriter avroFileWriter = new AvroFileWriter();
        avroFileWriter.writeAvroSchemaToFile(result.getSchema(), new PathCombine(Paths.get(destinationCachePath))
                .plus(AvroFileNames.SCHEMA_FOR_CACHE_DATA_FILENAME).getPath());

        Path cacheConfigurationsAvroFilePath = new PathCombine(Paths.get(destinationCachePath))
                .plus(AvroFileNames.CACHE_CONFIGURATION_FILENAME).getPath();
        Path configurationsSchemaAvroFilePath = new PathCombine(Paths.get(destinationCachePath))
                .plus(AvroFileNames.SCHEMA_FOR_CACHE_CONFIGURATION_FILENAME).getPath();
        Schema cacheConfigurationsSchema;

        cacheConfigurationsSchema = result.getCacheComponents()[0].getAvroFileReader().getCacheConfigurationsAvroSchema();

        AvroCacheConfigurationsWriter cacheConfigurationsWriter = new AvroCacheConfigurationsWriter.Builder()
                .setAvroSchema(cacheConfigurationsSchema)
                .setCacheConfigurationsAvroFilePath(cacheConfigurationsAvroFilePath)
                .setCacheConfigurations(cacheConfigConverter.convertFromEntity(result.getCacheConfiguration()))
                .setCacheQueryEntities(queryEntityConverter.convertFromEntity(result.getQueryEntities()))
                .setCacheDataTypes(result.getCacheDataTypes())
                .build();
        cacheConfigurationsWriter.write();

        avroFileWriter.writeAvroSchemaToFile(cacheConfigurationsSchema, configurationsSchemaAvroFilePath);
    }
}
