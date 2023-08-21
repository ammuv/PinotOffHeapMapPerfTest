package org.example;

import java.io.Serializable;
import java.util.Arrays;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;


public class Record implements Serializable {
  private final Object[] _values;

  public Record(Object[] values) {
    _values = values;
  }

  public Object[] getValues() {
    return _values;
  }

  // NOTE: Not check class for performance concern
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return Arrays.equals(_values, ((Record) o)._values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_values);
  }

  public static void main(String[] args) {

    try (ChronicleSet<Record> recordSet = ChronicleSetBuilder
        .of(Record.class)
        .averageKeySize(500)
        .entries(10)
        .create()) {


      String[] str1 = {"abc","bcd"};
      Integer[] int1 = {100,300,400};
      Record r1 = new Record(str1);
      Record r2 = new Record(int1);
      Record r3 = new Record(str1);

      recordSet.add(r1);
      recordSet.add(r2);

      boolean containsRecord3 = recordSet.contains(r3);
      System.out.println("Contains record3 (should be yes coz 1 and 3 are same): " + containsRecord3);

      // Iterate over the set
      for (Record r : recordSet) {
        System.out.println("Record in set: " + r);
      }
    }
  }
}