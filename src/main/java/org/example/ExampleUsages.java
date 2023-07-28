package org.example;

import java.util.Set;
import net.openhft.chronicle.set.ChronicleSet;
import java.util.Random;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;


public class ExampleUsages {

  void testChronicleSetLong(){
    Set<Long> set = ChronicleSetBuilder.of(Long.class)
        .entries(10)
        .create();
    final Random random = new Random();
    for (int i = 0; i < 300; i++) {
      set.add(random.nextLong());
      System.out.println(set.size());
    }
  }

  void testChronicleSetString(){
    String str = StringUtils.repeat("a", 5000);
    ChronicleSet<String> stringSet = ChronicleSetBuilder.of(String.class).entries(100000).averageKey(str).create();

    // populate sets for iterator and contains workloads
    for(int value=0;value<100000;++value){
      String s = RandomStringUtils.randomAlphabetic(1,100000);
      stringSet.add(s);
      System.out.println(s + " " + stringSet.size());
      System.out.println(stringSet.offHeapMemoryUsed());
    }
  }

  void testMapDBInt(DB db){
    Set<Integer> set= db.hashSet("int").serializer(Serializer.INTEGER).createOrOpen();
    // populate sets for iterator and contains workloads
    for(int value=0;value<5000;++value){
      set.add(value);
      //System.out.println(value + " " + set.size());
    }
  }

  void testMapDBString(DB db){
    Set<String> stringSet= db.hashSet("string").serializer(Serializer.STRING).createOrOpen();
    for(int value=0;value<30;++value){
      String s = RandomStringUtils.randomAlphabetic(1,10);
      stringSet.add(s);
     // System.out.println(stringSet.size());
    }
  }

 void testMapDBByteArray(DB db){
    Set<byte[]> set= db.hashSet("byte").serializer(Serializer.BYTE_ARRAY).createOrOpen();
    RandomUtils random = new RandomUtils();
    random.buildByteArraySetRandomRange(set,5000,10);
    System.out.println(set.size());
  }

  public static void main(String[] args){
    ExampleUsages ex = new ExampleUsages();
    //ex.testChronicleSetLong();
    //ex.testChronicleSetString();;

    DB db = DBMaker.memoryDirectDB().make();
    ex.testMapDBString(db);
    ex.testMapDBInt(db);
    ex.testMapDBByteArray(db);

    //DB db = DBMaker
    // .memoryDirectDB().make();
    //.allocateStartSize( 10 * 1024*1024*1024)  // 10GB
    //.allocateIncrement(512 * 1024*1024)
    //.make()

    //db.hashSet("set").layout() - can specify levels here

    // .keySerializer(Serializer.BYTE_ARRAY)

//    private DB db = DBMaker
//        .memoryDirectDB()
//        .allocateStartSize( 10 * 1024*1024*1024)  // 10GB
//        .allocateIncrement(512 * 1024*1024)
//        .make();
//
//    //Set<String> set = db.hashSet("mySet").createOrOpen();
//
//    NavigableSet<String> set = db
//        .treeSet("mySet")
//        .serializer(Serializer.STRING)
//        .createOrOpen();

  }

}
