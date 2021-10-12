package org.alliedium.ignite.migration.patchtools;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;
import java.util.stream.Collectors;

public class RenameCacheAction implements TransformAction<TransformOutput> {

    private final TransformAction<TransformOutput> action;
    private String newName;
    private String newTableName;

    public RenameCacheAction(TransformAction<TransformOutput> action) {
        this.action = action;
    }

    public RenameCacheAction newName(String newName) {
        this.newName = newName;
        return this;
    }

    public RenameCacheAction newTableName(String newTableName) {
        this.newTableName = newTableName;
        return this;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput output = action.execute();

        CacheConfiguration<Object, BinaryObject> cacheConfiguration = output.getCacheConfiguration();
        cacheConfiguration.setName(newName);

        Collection<QueryEntity> queryEntityCollection = output.getQueryEntities().stream()
                .map(queryEntity -> queryEntity.setTableName(newTableName))
                .collect(Collectors.toList());

        cacheConfiguration.setQueryEntities(queryEntityCollection);

        return new TransformOutput.Builder(output)
                .setCacheConfiguration(cacheConfiguration)
                .setQueryEntities(queryEntityCollection)
                .build();
    }
}
