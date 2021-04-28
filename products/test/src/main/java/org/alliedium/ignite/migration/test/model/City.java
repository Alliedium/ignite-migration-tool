package org.alliedium.ignite.migration.test.model;

import java.util.Objects;

public class City {
    String name, district;
    int population;

    public City(String name, String district, int population) {
        this.name = name;
        this.district = district;
        this.population = population;
    }

    public String getName() {
        return name;
    }

    public String getDistrict() {
        return district;
    }

    public int getPopulation() {
        return population;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public void setPopulation(int population) {
        this.population = population;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "City{" +
            "name='" + name + '\'' +
            ", district='" + district + '\'' +
            ", population=" + population +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        City city = (City) o;

        if (population != city.population) return false;
        if (!Objects.equals(name, city.name)) return false;
        return Objects.equals(district, city.district);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (district != null ? district.hashCode() : 0);
        result = 31 * result + population;
        return result;
    }
}
