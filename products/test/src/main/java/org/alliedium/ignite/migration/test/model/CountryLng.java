package org.alliedium.ignite.migration.test.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class CountryLng {
    @QuerySqlField
    String language;
    @QuerySqlField
    String isOfficial;
    @QuerySqlField
    double percentage;

    public CountryLng(String language, String isOfficial, double percentage) {
        this.language = language;
        this.isOfficial = isOfficial;
        this.percentage = percentage;
    }
}