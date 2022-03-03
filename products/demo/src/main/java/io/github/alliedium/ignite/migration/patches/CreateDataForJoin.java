package io.github.alliedium.ignite.migration.patches;

import io.github.alliedium.ignite.migration.demotools.CacheNames;
import io.github.alliedium.ignite.migration.demotools.CreateDataCommon;
import io.github.alliedium.ignite.migration.test.model.Flight;
import io.github.alliedium.ignite.migration.test.model.FlightFactory;
import io.github.alliedium.ignite.migration.test.model.FlightProfit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class CreateDataForJoin extends CreateDataCommon {

    public CreateDataForJoin(String[] args) {
        super(args);
    }

    @Override
    protected void createData() {
        List<Flight> flights = FlightFactory.createFlights(5);
        CacheConfiguration<Integer, Flight> flightCacheConfig = new CacheConfiguration<>();
        flightCacheConfig.setName(CacheNames.FIRST);
        IgniteCache<Integer, Flight> flightCache = clientAPI.getIgnite().createCache(flightCacheConfig);
        for (int i = 0; i < flights.size(); i++) {
            flightCache.put(i, flights.get(i));
        }

        Random random = new Random();

        CacheConfiguration<Integer, FlightProfit> flightProfitCacheConfiguration = new CacheConfiguration<>();
        flightProfitCacheConfiguration.setName(CacheNames.SECOND);
        IgniteCache<Integer, FlightProfit> flightProfitCache = clientAPI.getIgnite().createCache(flightProfitCacheConfiguration);
        flights.forEach(flight ->
            flightProfitCache.put(flight.getId(),
                    FlightProfit.builder()
                            .id(flight.getId())
                            .expense(random.nextInt(100000) + 1)
                            .income(random.nextInt(150000) + 50000)
                            .build()));
    }

    public static void main(String[] args) throws IOException {
        new CreateDataForJoin(args).execute();
    }
}
