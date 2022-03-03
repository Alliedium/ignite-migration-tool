package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.demotools.LoadAndCheckDataCommon;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

import java.util.Objects;

public class LoadDataFromAvroAndCheckPatchApplied extends LoadAndCheckDataCommon {

    private static final String fieldNameToAdd = "age";
    private static final String fieldNameToRemove = "population";

    public LoadDataFromAvroAndCheckPatchApplied(String[] args) {
        super(args);
    }

    @Override
    protected void checkData() {
        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.FIRST).size() > 0 ? true : null, "cache should not be empty");
        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.SECOND).size() > 0 ? true : null, "cache should not be empty");
        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.THIRD).size() > 0 ? true : null, "cache should not be empty");

        clientAPI.getIgnite().cache(CacheNames.FIRST).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Objects.requireNonNull(entry.getValue().field(fieldNameToAdd), "ignite cache objects should contain new field");
                });
        String message = String.format("--- checked that field [%s] was added successfully into cache [%s] ---",
                fieldNameToAdd, CacheNames.FIRST);
        System.out.println(message);
        resultWriter.writeLine(message);

        clientAPI.getIgnite().cache(CacheNames.SECOND).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    if (entry.getValue().field(fieldNameToRemove) != null) {
                        throw new IllegalStateException("ignite cache objects should contain new field");
                    }
                });
        message = String.format("--- checked that field [%s] was removed successfully from cache [%s] ---",
                fieldNameToRemove, CacheNames.SECOND);
        System.out.println(message);
        resultWriter.writeLine(message);
    }

    public static void main(String[] args) {
        new LoadDataFromAvroAndCheckPatchApplied(args).execute();
    }
}
