package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.serializer.AvroFileWriter;
import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.alliedium.ignite.migration.util.PathCombine;
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

public class Writer {
    private static final IIgniteDTOConverter<String, Collection<QueryEntity>> queryEntityConverter =
            IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER;
    private static final IIgniteDTOConverter<String, CacheConfiguration<Object, BinaryObject>> cacheConfigConverter =
            IgniteObjectStringConverter.CACHE_CONFIG_CONVERTER;
    private final TransformAction<TransformOutput> transformAction;

    public Writer(TransformAction<TransformOutput> transformAction) {
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

        avroFileWriter.writeCacheConfigurationsToFile(cacheConfigurationsSchema, cacheConfigurationsAvroFilePath,
                cacheConfigConverter.convertFromEntity(result.getCacheConfiguration()),
                queryEntityConverter.convertFromEntity(result.getQueryEntities()));
        avroFileWriter.writeAvroSchemaToFile(cacheConfigurationsSchema, configurationsSchemaAvroFilePath);
    }
}
