package org.alliedium.ignite.migration.test.model;

public class Passport {
    String id;

    public Passport(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Passport passport = (Passport) o;

        return id != null ? id.equals(passport.id) : passport.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Passport{" +
                "id='" + id + '\'' +
                '}';
    }
}
