package org.example;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
/*
  Baseline Benchmark ChronicleSet for all our Workloads
  Note ChronicleSet always needs upper bound on number of entries in the Set
 */

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 30)
@Measurement(iterations = 3, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkMapDB{
  private static final int NUM_KEYS_PRELOAD = 1000000;
  private static final int GB_TO_BYTES = 1024*1024*1024; // conversion constant
  private static final int COLLISION_FACTOR = 100;  //about COLLISION_FACTOR many collisions per key for collision workloads
  @Param({"0.005"}) //GB of data to store
  float _gb;

  private RandomUtils _random;
  private Set<Integer> _intSet;
  private Set<String>  _stringSet;
  private DB _db;
  @Setup
  public void setUp(){
    _random = new RandomUtils();
    _db = DBMaker.memoryDirectDB().make();
    _intSet = _db.hashSet("int").serializer(Serializer.INTEGER).createOrOpen();

    String str = StringUtils.repeat("a", 50);
    _stringSet = _db.hashSet("string").serializer(Serializer.STRING).createOrOpen();

    // populate sets for iterator and contains workloads
    for(int value=0;value<NUM_KEYS_PRELOAD;++value)
      _intSet.add(value);

    // populate sets for iterator and contains workloads
    _random.buildStringSetRandomRange(_stringSet,NUM_KEYS_PRELOAD,100);

    //System.out.println("Setup int array " + _stringSet.size());
    //System.out.println("Setup string array " + _intSet.size());
  }

  @TearDown
  public void tearDown(){
    _db.close();
  }
  /*
    Workload: Int Sorted with no collision
    Number of entries : based on _gb of storage
   */
  @Benchmark
  public void insertIntSortedNoCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;

    //DB db = DBMaker.memoryDirectDB().make();
    Set<Integer> set = _db.hashSet("int1").serializer(Serializer.INTEGER).createOrOpen();

    int value;
    for(value=0;value<numEntries;++value)
      set.add(value);

    //System.out.println("insertIntSortedNoCollision: " + set.size());
  }

  /*
    Workload: Int Sorted with collision
    Number of entries : based on _gb of storage
    Collision: COLLISION_FACTOR many collision per key
   */
  @Benchmark
  public void insertIntSortedCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;

    //DB db = DBMaker.memoryDirectDB().make();
    Set<Integer> set = _db.hashSet("int2").serializer(Serializer.INTEGER).createOrOpen();
    int maxValue = (int) numEntries/COLLISION_FACTOR;
    int value,count=0;

    for(value=0;value<=maxValue;++value) {
      while(count<(value*COLLISION_FACTOR) && count<numEntries) {
        set.add(value);
        ++count;
      }
    }
    //System.out.println("insertIntSortedCollision: " + set.size());
  }

  /*
    Workload: Int Random with collision
    Number of entries : based on _gb of storage
    Collision: COLLISION_FACTOR many collision per key
   */
  @Benchmark
  public void insertIntRandomCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;

    //DB db = DBMaker.memoryDirectDB().make();
    Set<Integer> set = _db.hashSet("int3").serializer(Serializer.INTEGER).createOrOpen();
    int maxValue = numEntries/COLLISION_FACTOR;
    _random.buildIntSetRandomRange(set,numEntries,maxValue);

    //System.out.println("insertIntRandomCollision: " + set.size());
  }

  /*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 150 so 1/2^25 probability of collision
  */
  @Benchmark
  public void insertStringRandomLowCollision(){
    int maxLength = 150;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2

    //DB db = DBMaker.memoryDirectDB().make();
    Set<String> set = _db.hashSet("string1").serializer(Serializer.STRING).createOrOpen();

    _random.buildStringSetRandomRange(set,numEntries,maxLength);

    //System.out.println("insertStringRandomLowCollision: " + set.size());
  }

  /*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 20 for about 1:10 collision
  */
  @Benchmark
  public void insertStringRandomCollision(){
    int maxLength = 20;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2

    //DB db = DBMaker.memoryDirectDB().make();
    Set<String> set = _db.hashSet("string2").serializer(Serializer.STRING).createOrOpen();

    _random.buildStringSetRandomRange(set,numEntries,maxLength);

   // System.out.println("insertStringRandomCollision: " + set.size());
  }

  /*
  Workload: Random Byte Array with minimum collision
  Number of entries : based on _gb of storage
  Collision: maxLength set to 20 for about 1:10 collision
  */
  @Benchmark
  public void insertByteArrayRandomLowCollision(){
    int maxLength = 120;
    int numEntries = (int)(GB_TO_BYTES*_gb*2)/maxLength;  // average length is maxLength/2

    //DB db = DBMaker.memoryDirectDB().make();
    Set<byte[]> set = _db.hashSet("byte1").serializer(Serializer.BYTE_ARRAY).createOrOpen();

    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);

   // System.out.println("insertByteArrayRandomLowCollision: " + set.size());
  }

  /*
  Workload: Random Byte Array with collision
  Number of entries : based on _gb of storage
  Collision: maxLength set to 10 for about 1:10 collision
  */
  @Benchmark
  public void insertByteArrayRandomCollision(){
    int maxLength = 10;
    int numEntries = (int)(GB_TO_BYTES*_gb*2)/maxLength;  // average length is maxLength/2

    //DB db = DBMaker.memoryDirectDB().make();
    Set<byte[]> set = _db.hashSet("byte2").serializer(Serializer.BYTE_ARRAY).createOrOpen();

    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);

   // System.out.println("insertByteArrayRandomCollision: " + set.size());
  }

  // ITERATOR WORKLOADS //
  @Benchmark
  public void iterateInt(){
    _intSet.iterator();
  }

  @Benchmark
  public void iterateString(){
    _stringSet.iterator();
  }

  //  CONTAINS WORKLOADS //
  @Benchmark
  public void containsIntWithinRange(){
    _intSet.contains(_random.getRandomInt(0,NUM_KEYS_PRELOAD));
  }

  @Benchmark
  public void containsIntOutsideRange(){
    _intSet.contains(_random.getRandomInt(NUM_KEYS_PRELOAD,1500000));
  }

  @Benchmark
  public void containsStringWithinRange(){
    _stringSet.contains(_random.generateRandomString(1,100));
  }

  @Benchmark
  public void containsStringOutsideRange(){
    _stringSet.contains(_random.generateRandomString(101,150));
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkMapDB.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}

