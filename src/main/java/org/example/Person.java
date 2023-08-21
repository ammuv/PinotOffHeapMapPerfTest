package org.example;

import java.io.Serializable;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;

public class Person implements Serializable {
  private String name;
  private int age;

  public Person(String name, int age) {
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public int getAge() {
    return age;
  }

  public String toPrint() {
    return "Person{" +
        "name='" + name + '\'' +
        ", age=" + age +
        '}';
  }

  public static void main(String[] args) {
    Person person1 = new Person("Alice", 25);
    try (ChronicleSet<Person> personSet = ChronicleSetBuilder
        .of(Person.class)
        .averageKey(person1)
        .entries(10)
        .create()) {


      Person person2 = new Person("Bob", 30);

      personSet.add(person1);
      personSet.add(person2);

      boolean containsPerson1 = personSet.contains(person1);
      System.out.println("Contains person1: " + containsPerson1);

      // Iterate over the set
      for (Person person : personSet) {
        System.out.println("Person in set: " + person.toPrint());
      }
    }
  }
}
