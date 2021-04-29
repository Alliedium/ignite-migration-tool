package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.test.model.City;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void serializeDeserializeObjects() {
        for (int i = 0; i < 100_000; i++) {
            City city = new City("city_name", "test_district", 300);
            String serializedCity = Utils.serializeObjectToXML(city);
            City city1 = Utils.deserializeFromXML(serializedCity);
            Assert.assertEquals(city, city1);
        }
    }
}