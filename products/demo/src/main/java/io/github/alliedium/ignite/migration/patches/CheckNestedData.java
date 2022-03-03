package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.demotools.LoadAndCheckDataCommon;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

import java.util.Collection;
import java.util.Objects;

public class CheckNestedData extends LoadAndCheckDataCommon {
    public CheckNestedData(String[] args) {
        super(args);
    }

    @Override
    protected void checkData() {
        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.NESTED_DATA_CACHE).size() > 0 ? true : null, "cache should not be empty");
        clientAPI.getIgnite().cache(CacheNames.NESTED_DATA_CACHE).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    if (entry.getValue().field("persons") == null) {
                        throw new IllegalStateException("ignite cache objects should contain new field");
                    }
                    Collection<BinaryObject> persons = entry.getValue().field("persons");
                    String personName = persons.iterator().next().field("name");
                    if (!personName.contains("changed")) {
                        throw new IllegalStateException("first person should contain 'changed' word in it's name");
                    }
                });
    }

    public static void main(String[] args) {
        new CheckNestedData(args).execute();
    }
}
