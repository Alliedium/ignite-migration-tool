package org.alliedium.ignite.migration.patchtools;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public class RenameCacheAction implements TransformAction<TransformOutput> {

    private final TransformAction<TransformOutput> action;
    private final String newCacheName;
    private final String newTableName;

    private RenameCacheAction(Builder builder) {
        action = builder.action;
        newCacheName = builder.newCacheName;
        newTableName = builder.newTableName;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput output = action.execute();

        CacheConfiguration<Object, BinaryObject> cacheConfiguration = output.getCacheConfiguration();
        cacheConfiguration.setName(newCacheName);

        Collection<QueryEntity> queryEntityCollection = output.getQueryEntities().stream()
                .map(queryEntity -> queryEntity.setTableName(newTableName))
                .collect(Collectors.toList());

        cacheConfiguration.setQueryEntities(queryEntityCollection);

        return new TransformOutput.Builder(output)
                .setCacheConfiguration(cacheConfiguration)
                .setQueryEntities(queryEntityCollection)
                .build();
    }

    public static class Builder {
        private TransformAction<TransformOutput> action;
        private String newCacheName;
        private String newTableName;

        public Builder action(TransformAction<TransformOutput> action) {
            this.action = action;
            return this;
        }

        public Builder newCacheName(String newCacheName) {
            this.newCacheName = newCacheName;
            return this;
        }

        public Builder newTableName(String newTableName) {
            this.newTableName = newTableName;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(action, "parent action is null");
            Objects.requireNonNull(newCacheName, "new cache name not set");
            Objects.requireNonNull(newTableName, "new table name not set");
        }

        public RenameCacheAction build() {
            validate();
            return new RenameCacheAction(this);
        }
    }
}
