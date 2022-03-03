package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.demotools.CreateDataCommon;

import java.io.IOException;

public class CreateIgniteDataAndWriteIntoAvro extends CreateDataCommon {

    public CreateIgniteDataAndWriteIntoAvro(String[] args) {
        super(args);
    }

    @Override
    protected void createData() {
        clientAPI.createTestCityCacheAndInsertData(CacheNames.FIRST, 100);
        clientAPI.createTestCityCacheAndInsertData(CacheNames.SECOND, 100);
        clientAPI.createTestCityCacheAndInsertData(CacheNames.THIRD, 100);
    }

    public static void main(String[] args) throws IOException {
        new CreateIgniteDataAndWriteIntoAvro(args).execute();
    }
}
