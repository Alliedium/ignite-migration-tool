package io.github.alliedium.ignite.migration.demotools;

import io.github.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import io.github.alliedium.ignite.migration.Controller;
import io.github.alliedium.ignite.migration.IgniteConfigLoader;
import io.github.alliedium.ignite.migration.test.ClientAPI;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class LoadDataFromAvroAndCheckPatchApplied {

    private static final String fieldNameToAdd = "age";
    private static final String fieldNameToRemove = "population";

    public static void main(String[] args) {
        ClientAPI clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        Path serializedDataPath = clientAPI.getAvroMainPath();
        if (args.length > 0) {
            serializedDataPath = Paths.get(args[0]);
        }

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        Controller controller = new Controller(clientAPI.getIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        controller.deserializeDataFromAvro(serializedDataPath);

        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.FIRST).size() > 0 ? true : null, "cache should not be empty");
        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.SECOND).size() > 0 ? true : null, "cache should not be empty");
        Objects.requireNonNull(clientAPI.getIgnite().cache(CacheNames.THIRD).size() > 0 ? true : null, "cache should not be empty");

        clientAPI.getIgnite().cache(CacheNames.FIRST).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Objects.requireNonNull(entry.getValue().field(fieldNameToAdd), "ignite cache objects should contain new field");
                });
        System.out.println(
                String.format("--- checked that field [%s] was added successfully into cache [%s] ---",
                        fieldNameToAdd, CacheNames.FIRST));

        clientAPI.getIgnite().cache(CacheNames.SECOND).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    if (entry.getValue().field(fieldNameToRemove) != null) {
                        throw new IllegalStateException("ignite cache objects should contain new field");
                    }
                });
        System.out.println(
                String.format("--- checked that field [%s] was removed successfully from cache [%s] ---",
                        fieldNameToRemove, CacheNames.SECOND));

        clientAPI.getIgnite().close();
    }
}
