package org.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;
import java.util.Objects;

public class TransformOutput implements DataComponentsProvider {
    private final PCollection<Row> pCollection;
    private final CacheComponent[] cacheComponents;
    private final String[] fields;
    private final Schema schema;
    private final CacheConfiguration<Object, BinaryObject> cacheConfiguration;
    private final Collection<QueryEntity> queryEntities;

    private TransformOutput(Builder builder) {
        this.pCollection = builder.pCollection;
        this.cacheComponents = builder.cacheComponents;
        this.fields = builder.fields;
        this.schema = builder.schema;
        this.cacheConfiguration = builder.cacheConfiguration;
        this.queryEntities = builder.queryEntities;
    }

    public static class Builder {
        private PCollection<Row> pCollection;
        private CacheComponent[] cacheComponents;
        private String[] fields;
        private Schema schema;
        private CacheConfiguration<Object, BinaryObject> cacheConfiguration;
        private Collection<QueryEntity> queryEntities;

        public Builder() {
        }

        public Builder(TransformOutput other) {
            setCacheComponents(other.getCacheComponents().clone());
            setSchema(other.getSchema());
            setFields(other.getFields().clone());
            setPCollection(other.getPCollection());
            setQueryEntities(other.getQueryEntities());
            setCacheConfiguration(other.getCacheConfiguration());
        }

        public Builder setPCollection(PCollection<Row> pCollection) {
            this.pCollection = pCollection;
            return this;
        }

        public Builder setCacheComponents(CacheComponent... cacheComponents) {
            this.cacheComponents = cacheComponents.clone();
            return this;
        }

        public Builder setFields(String[] fields) {
            this.fields = fields.clone();
            return this;
        }

        public Builder setSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setCacheConfiguration(CacheConfiguration<Object, BinaryObject> cacheConfiguration) {
            this.cacheConfiguration = cacheConfiguration;
            return this;
        }

        public Builder setQueryEntities(Collection<QueryEntity> queryEntities) {
            this.queryEntities = queryEntities;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(cacheComponents, "cacheComponents should not be null");
            Objects.requireNonNull(pCollection, "pCollection should not be null");
            Objects.requireNonNull(fields, "fields should not be null");
            Objects.requireNonNull(schema, "schema should not be null");
            Objects.requireNonNull(cacheConfiguration, "cacheConfiguration should not be null");
            Objects.requireNonNull(queryEntities, "queryEntities should not be null");
            if (cacheComponents.length < 1) {
                throw new IllegalArgumentException("At list one cache component should be provided");
            }
        }

        public TransformOutput build() {
            validate();
            return new TransformOutput(this);
        }
    }

    @Override
    public PCollection<Row> getPCollection() {
        return pCollection;
    }

    @Override
    public CacheComponent getFirstCacheComponent() {
        return cacheComponents[0];
    }

    public CacheComponent[] getCacheComponents() {
        return cacheComponents.clone();
    }

    public String[] getFields() {
        return fields.clone();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    public CacheConfiguration<Object, BinaryObject> getCacheConfiguration() {
        return cacheConfiguration;
    }

    public Collection<QueryEntity> getQueryEntities() {
        return queryEntities;
    }
}
