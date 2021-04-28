package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.Controller;
import org.alliedium.ignite.migration.IgniteConfigLoader;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.test.ClientAPI;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateIgniteDataAndWriteIntoAvro {
    private static final String cacheName = "testCache";

    public static void main(String[] args) throws IOException {
        ClientAPI clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        Path pathToSerialize = clientAPI.getAvroTestSetPath();
        if (args.length > 0) {
            pathToSerialize = Paths.get(args[0]);
        }

        clientAPI.cleanIgniteAndRemoveDirectories();
        clientAPI.deleteDirectoryRecursively(pathToSerialize);
        clientAPI.deleteDirectoryRecursively(clientAPI.getAvroMainPath());
        clientAPI.createTestCityCacheAndInsertData(cacheName, 100);

        Controller controller = new Controller(clientAPI.getIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(pathToSerialize);
        clientAPI.getIgnite().close();
    }
}
