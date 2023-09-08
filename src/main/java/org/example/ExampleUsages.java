package org.example;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.set.ChronicleSet;
import java.util.Random;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class ExampleUsages {

  void testChronicleSetLong(){
    Set<Long> set = ChronicleSetBuilder.of(Long.class)
        .entries(300)
        .create();
    final Random random = new Random();
    for (int i = 0; i < 300; i++) {
      set.add(random.nextLong());
      System.out.println(set.size());
    }
  }

  private static String toMB(long init) {
    return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
  }
  void testChronicleSetString(){
    String str = StringUtils.repeat("a", 5000);
    ChronicleSet<String> stringSet = ChronicleSetBuilder.of(String.class).entries(10000).averageKey(str).create();

    // populate sets for iterator and contains workloads
    for(int value=0;value<10000;++value) {
      String s = RandomStringUtils.randomAlphabetic(1, 10000);
      stringSet.add(s);
    }
    System.out.println(toMB(stringSet.offHeapMemoryUsed()));
      //System.out.println(s + " " + stringSet.size());
    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (BufferPoolMXBean pool : pools) {
      System.out.println(pool.getName());
      System.out.println(pool.getCount());
      System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
      System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
      System.out.println();
    }
  }

  void testChronicleAddAll(){
    Set<Long> set = ChronicleSetBuilder.of(Long.class)
        .entries(50)
        .create();
    final Random random = new Random();
    for (int i = 0; i < 50; i++) {
      set.add(random.nextLong());
    }
    System.out.println("Set1: "+set.size());
    Set<Long> set2 = ChronicleSetBuilder.of(Long.class)
        .entries(75)
        .create();
    set2.addAll(set);
    System.out.println("Set2: "+set2.size());
  }

  void testMemoryOffHeapChronicle(){
    ChronicleSet<Integer> set = ChronicleSetBuilder.of(Integer.class)
        .entries(10000000)
        .create();
    System.out.println(toMB(set.offHeapMemoryUsed()));
    for (int i = 0; i < 10000000; i++) {
      set.add(i);
    }
    System.out.println(toMB(set.offHeapMemoryUsed()));
    //System.out.println(s + " " + stringSet.size());
    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (BufferPoolMXBean pool : pools) {
      System.out.println(pool.getName());
      System.out.println(pool.getCount());
      System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
      System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
      System.out.println();
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


  void testMemoryOffHeapMapDB(DB db){
    Set<Integer> set= db.hashSet("int").serializer(Serializer.INTEGER).createOrOpen();

    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (int i = 0; i < 10000000; i++) {
      set.add(i);
    }
    //System.out.println(s + " " + stringSet.size());
    for (BufferPoolMXBean pool : pools) {
      System.out.println(pool.getName());
      System.out.println(pool.getCount());
      System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
      System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
      System.out.println();
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

  void testMapDBObjectClass(DB db){
    Set<String> stringSet = db.hashSet("string").serializer(Serializer.STRING).createOrOpen();
    System.out.println(stringSet.getClass());
    System.out.println(HTreeMap.KeySet.class);
    if(stringSet instanceof HTreeMap.KeySet)
      System.out.println("matches");
  }
  void testMapDB(){
    DB db = DBMaker.memoryDirectDB().make();
    //testMapDBString(db);
    //testMapDBInt(db);
    //testMapDBByteArray(db);
   // testMapDBObjectClass(db);
    testMemoryOffHeapMapDB(db);
  }

  void testChronicleKeyType(){
    ChronicleSet intSet = ChronicleSetBuilder.of(Integer.class).entries(50).create();
    ChronicleSet longSet = ChronicleSetBuilder.of(Long.class).entries(50).create();
    ChronicleSet doubleSet = ChronicleSetBuilder.of(Double.class).entries(50).create();
    ChronicleSet floatSet = ChronicleSetBuilder.of(Float.class).averageKey((float) 4.67).entries(50).create();
    ChronicleSet stringSet = ChronicleSetBuilder.of(String.class).entries(50).averageKey("abc").create();
    ChronicleSet byteSet = ChronicleSetBuilder.of(byte[].class).averageKey(new byte[5]).entries(50).create();

    System.out.println("int "+intSet.keyType());
    System.out.println("long "+longSet.keyType());
    System.out.println("double "+doubleSet.keyType());
    System.out.println("int "+floatSet.keyType());
    System.out.println("string "+stringSet.keyType());
    System.out.println("byte "+ byteSet.keyType() + byteSet.keyClass());

    if(intSet.keyType()==Integer.TYPE)
      System.out.println("int type " + intSet.keyType());
    if(intSet.keyClass()==Integer.class)
      System.out.println("int class "+ intSet.keyType());
    if(intSet.keyType()==Integer.class)
      System.out.println("int type class "+ intSet.keyType());
    if(intSet.keyClass().equals(Integer.class)){
      System.out.println("works");
    }

    if(byteSet.keyType().equals(byte[].class)){
      System.out.println("byte works");
    }

    if(byteSet.keyClass().equals(byte[].class)){
      System.out.println("byte works");
    }

  }

  void testChronicleIterate(){
    ChronicleSet set = ChronicleSetBuilder.of(Long.class)
        .entries(300)
        .create();
    for (int i = 0; i < 10; i++) {
      set.add((long) i);
    }
    Iterator<Long> it = set.iterator();
    while(it.hasNext()){
      long l = it.next();
      System.out.println(l);
    }
    System.out.println();
    it = set.iterator();
    while(it.hasNext()){
      long l = it.next();
      System.out.println(l);
    }

  }

  void testRecordChronicle(){
    String[] str = {"abc","bcd"};
    Record r = new Record(str);
    Set<Record> set = ChronicleSetBuilder.of(Record.class).averageKey(r).entries(50).create();
    set.add(r);
    System.out.println(set.size());
  }
  void testChronicle(){
    //testChronicleAddAll();
    //testChronicleSetLong();
    //testChronicleSetString();;
    testChronicleKeyType();
    //testChronicleIterate();
    //testRecordChronicle();
    //testMemoryOffHeapChronicle();
  }

  public static void main(String[] args){
    ExampleUsages ex = new ExampleUsages();

    //ex.testMapDB();
    ex.testChronicle();
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
  }

}
