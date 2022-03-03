package io.github.alliedium.ignite.migration.test.model;

import java.util.*;

public class FlightFactory {

    private static final Random random = new Random();

    public static Flight create() {
        return create(3);
    }

    public static List<Flight> createFlights(int flightCount) {
        List<Flight> flights = new ArrayList<>();
        for (int i = 0; i < flightCount; i++) {
            flights.add(create());
        }
        return flights;
    }

    public static Flight create(int personCount) {
        List<Person> personList = new ArrayList<>();
        Map<Person, Integer> tickets = new HashMap<>();
        for (int i = 0; i < personCount; i++) {
            Passport passport = generatePassport();
            Person person = generatePerson(passport);
            personList.add(person);
            tickets.put(person, nextTicketNumber());
        }

        return Flight.builder()
                .id(nextInt())
                .personList(personList)
                .tickets(tickets)
                .build();
    }

    private static Passport generatePassport() {
        return new Passport("B" + nextInt());
    }
    private static Person generatePerson(Passport passport) {
        return new Person("Person" + nextInt(), passport, nextAge());
    }

    private static int nextInt() {
        return random.nextInt(90000) + 10000;
    }

    private static int nextAge() {
        return random.nextInt(84) + 16;
    }

    private static int nextTicketNumber() {
        return random.nextInt(100000) + 100000;
    }
}
