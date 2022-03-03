package io.github.alliedium.ignite.migration.test.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FlightProfit {
    private final int id;
    private final int expense;
    private final int income;
}
