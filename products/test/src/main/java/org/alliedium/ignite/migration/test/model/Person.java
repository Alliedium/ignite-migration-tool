package org.alliedium.ignite.migration.test.model;

public class Person {
    String name;
    int age;
    Passport passport;

    public Person(String name, Passport passport, int age) {
        this.name = name;
        this.passport = passport;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public Passport getPassport() {
        return passport;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPassport(Passport passport) {
        this.passport = passport;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", passport=" + passport +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Person person = (Person) o;

        if (age != person.age) return false;
        if (name != null ? !name.equals(person.name) : person.name != null) return false;
        return passport != null ? passport.equals(person.passport) : person.passport == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + age;
        result = 31 * result + (passport != null ? passport.hashCode() : 0);
        return result;
    }
}
