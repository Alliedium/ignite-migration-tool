package io.github.alliedium.ignite.migration.test.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class CountryLngKey {

    /** */
    @AffinityKeyMapped
    private String COUNTRYCODE;

    private String LANGUAGE;

    public CountryLngKey(String COUNTRYCODE, String LANGUAGE) {
        this.COUNTRYCODE = COUNTRYCODE;
        this.LANGUAGE = LANGUAGE;
    }
}
