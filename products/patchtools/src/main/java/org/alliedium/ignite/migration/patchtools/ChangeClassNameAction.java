package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.dto.CacheDataTypes;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;
import java.util.Objects;

public class ChangeClassNameAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> action;
    private final String fromClassName;
    private final String toClassName;

    private ChangeClassNameAction(Builder builder) {
        this.action = builder.action;
        this.fromClassName = builder.fromClassName;
        this.toClassName = builder.toClassName;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput output = action.execute();

        Collection<QueryEntity> queryEntities = output.getQueryEntities();
        queryEntities.forEach(queryEntity -> {
            if (queryEntity.getValueType().equals(fromClassName)) {
                queryEntity.setValueType(toClassName);
            }
        });
        CacheConfiguration<Object, BinaryObject> cacheConfiguration = output.getCacheConfiguration();
        cacheConfiguration.setQueryEntities(queryEntities);

        CacheDataTypes cacheDataTypes = output.getCacheDataTypes();
        CacheDataTypes newCacheDataTypes = new CacheDataTypes(cacheDataTypes.getKeyType(), toClassName);

        return new TransformOutput.Builder(output)
                .setCacheConfiguration(cacheConfiguration)
                .setQueryEntities(queryEntities)
                .setCacheDataTypes(newCacheDataTypes)
                .build();
    }

    public static class Builder {
        private TransformAction<TransformOutput> action;
        private String fromClassName;
        private String toClassName;

        public Builder action(TransformAction<TransformOutput> action) {
            this.action = action;
            return this;
        }

        public Builder changeClassName(String fromClassName, String toClassName) {
            this.fromClassName = fromClassName;
            this.toClassName = toClassName;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(action, "parent action is null");
            Objects.requireNonNull(fromClassName, "from class name field is not set");
            Objects.requireNonNull(toClassName, "to class name field is not set");
        }

        public ChangeClassNameAction build() {
            validate();
            return new ChangeClassNameAction(this);
        }
    }
}
