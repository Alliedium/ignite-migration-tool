package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.AlterDataCommon;
import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.patchtools.*;

import java.io.IOException;

public class MakeJoin extends AlterDataCommon {
    public MakeJoin(String[] args) {
        super(args);
    }

    @Override
    protected void alterData() {
        context.patchCachesWhichEndWith(CacheNames.FIRST, cachePath -> {
            String flightProfitPath = cachePath.substring(0, cachePath.lastIndexOf("/") + 1)
                    + CacheNames.SECOND;
            SelectAction flightAction = new SelectAction.Builder()
                    .fields("key", "id", "tickets")
                    .context(context)
                    .from(cachePath)
                    .build();
            SelectAction flightProfitAction = new SelectAction.Builder()
                    .fields("id", "expense", "income")
                    .context(context)
                    .from(flightProfitPath)
                    .build();

            TransformAction<TransformOutput> action = new JoinAction.Builder()
                    .join(flightAction, flightProfitAction)
                    .on("id")
                    .build();

            action = new RenameCacheAction.Builder()
                    .action(action)
                    .newTableName(CacheNames.THIRD)
                    .newCacheName(CacheNames.THIRD)
                    .build();

            new CacheWriter(action).writeTo(destination.plus(CacheNames.THIRD).getPath().toString());
        });
        context.getPipeline().run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException {
        new MakeJoin(args).execute();
    }
}
