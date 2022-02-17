package io.github.alliedium.ignite.migration;

import io.github.alliedium.ignite.migration.test.model.City;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {

    @Test
    public void serializeDeserializeObjects() {
        for (int count = 0; count < 100_000; count++) {
            City city = new City("city_name", "test_district", 300);
            String serializedCity = Utils.serializeObjectToXML(city);
            City city1 = Utils.deserializeFromXML(serializedCity);
            Assert.assertEquals(city, city1);
        }
    }
}