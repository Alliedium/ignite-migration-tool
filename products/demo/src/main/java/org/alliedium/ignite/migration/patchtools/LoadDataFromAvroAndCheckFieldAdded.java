package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.Controller;
import org.alliedium.ignite.migration.IgniteConfigLoader;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.test.ClientAPI;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class LoadDataFromAvroAndCheckFieldAdded {

    private static final String cacheName = "testCache";
    private static final String fieldName = "age";

    public static void main(String[] args) {
        ClientAPI clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        Path serializedDataPath = clientAPI.getAvroMainPath();
        if (args.length > 0) {
            serializedDataPath = Paths.get(args[0]);
        }

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        Controller controller = new Controller(clientAPI.getIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        controller.deserializeDataFromAvro(serializedDataPath);

        Objects.requireNonNull(clientAPI.getIgnite().cache(cacheName).size() > 0 ? true : null, "cache should not be empty");

        clientAPI.getIgnite().cache(cacheName).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Objects.requireNonNull(entry.getValue().field(fieldName), "ignite cache objects should contain new field");
                });
        clientAPI.getIgnite().close();

        System.out.println(String.format("--- checked that field [%s] was added successfully ---", fieldName));
    }
}
