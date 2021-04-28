package org.alliedium.ignite.migration.test.model;

public class Country {
    private String code, name, continent, region, localName, governmentForm, headOfState, code2;
    private Double surfaceArea, lifeExpectancy, gNP, gNPOld;
    private Integer indepYear, population, capital;

    public Country(String code, String name, String continent, String region, String localName, String governmentForm,
        String headOfState, String code2, Double surfaceArea, Double lifeExpectancy, Double gNP,
        Double gNPOld, Integer indepYear, Integer population, Integer capital
    ) {
        this.code = code;
        this.name = name;
        this.continent = continent;
        this.region = region;
        this.localName = localName;
        this.governmentForm = governmentForm;
        this.headOfState = headOfState;
        this.code2 = code2;
        this.surfaceArea = surfaceArea;
        this.lifeExpectancy = lifeExpectancy;
        this.gNP = gNP;
        this.gNPOld = gNPOld;
        this.indepYear = indepYear;
        this.population = population;
        this.capital = capital;
    }
}
