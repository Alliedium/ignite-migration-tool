package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.AlterDataCommon;
import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.patchtools.*;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlterNestedData extends AlterDataCommon {


    public AlterNestedData(String[] args) {
        super(args);
    }

    @Override
    protected void alterData() {
        context.patchCachesWhichEndWith(CacheNames.NESTED_DATA_CACHE, cachePath -> {
            TransformAction<TransformOutput> action = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "id", "personList", "tickets")
                    .from(cachePath)
                    .build();

            action = new RenameFieldAction.Builder()
                    .action(action).renameField("personList", "persons")
                    .build();
            action = new MapAction.Builder()
                    .action(action)
                    .map(row -> {
                        List<Row> persons = row.getValue("persons");
                        List<Row> personsCopy = new ArrayList<>(persons);
                        String personName = personsCopy.get(0).getValue("name");
                        personsCopy.set(0, Row.fromRow(personsCopy.get(0))
                                .withFieldValue("name", personName + "changed").build());
                        return Row.fromRow(row)
                                .withFieldValue("persons", personsCopy)
                                .build();
                    })
                    .build();
            String cacheName = cachePath.substring(cachePath.lastIndexOf("/"));
            new CacheWriter(action).writeTo(destination.plus(cacheName).getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException {
        new AlterNestedData(args).execute();
    }
}
