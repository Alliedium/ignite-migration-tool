package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.demotools.CreateDataCommon;
import io.github.alliedium.ignite.migration.test.model.Flight;
import io.github.alliedium.ignite.migration.test.model.FlightFactory;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.IOException;
import java.util.List;

public class CreateNestedData extends CreateDataCommon {

    public CreateNestedData(String[] args) {
        super(args);
    }

    @Override
    protected void createData() {
        List<Flight> flights = FlightFactory.createFlights(5);
        CacheConfiguration<Integer, Flight> flightCacheConfig = new CacheConfiguration<>();
        flightCacheConfig.setName(CacheNames.NESTED_DATA_CACHE);
        IgniteCache<Integer, Flight> flightCache = clientAPI.getIgnite().createCache(flightCacheConfig);
        for (int i = 0; i < flights.size(); i++) {
            flightCache.put(i, flights.get(i));
        }
    }

    public static void main(String[] args) throws IOException {
        new CreateNestedData(args).execute();
    }
}
