package org.alliedium.ignite.migration.test.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Product {
    private final long id;
    private final long fabricatorId;
    private final String name;
    private final long price;
}
