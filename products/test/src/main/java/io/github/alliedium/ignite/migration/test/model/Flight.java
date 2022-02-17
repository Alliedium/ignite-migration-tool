package io.github.alliedium.ignite.migration.test.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class Flight {
    private final List<Person> personList;
    private final Map<Person, Integer> tickets;
}
