package org.alliedium.ignite.migration.test;

import org.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import org.apache.ignite.Ignite;

import java.util.ArrayList;
import java.util.List;

public class DefaultIgniteAtomicLongNamesProvider implements IgniteAtomicLongNamesProvider {

    private final Ignite ignite;

    public DefaultIgniteAtomicLongNamesProvider(Ignite ignite) {
        this.ignite = ignite;
    }

    public List<String> getAtomicNames() {
        List<String> cachesAtomicNames = new ArrayList<>();
        ignite.cacheNames().forEach(cacheName -> {
            cachesAtomicNames.add(cacheName + "_seq");
            cachesAtomicNames.add("PROD_" + cacheName + "_seq");
        });

        return cachesAtomicNames;
    }
}
