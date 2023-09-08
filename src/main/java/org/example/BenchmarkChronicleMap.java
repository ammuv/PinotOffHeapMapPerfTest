package org.example;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.openhft.chronicle.set.ChronicleSetBuilder;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
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
public class BenchmarkChronicleMap{
  private static final int NUM_KEYS_PRELOAD = 1000000;
  private static final int GB_TO_BYTES = 1024*1024*1024; // conversion constant
  private static final int COLLISION_FACTOR = 100;  //about COLLISION_FACTOR many collisions per key for collision workloads
  @Param({"0.05"}) //GB of data to store
  float _gb;

  @Param({"5"})
  //@Param({"5","20","60","150"}) // key length for variable length workloads
  int _keyLength;
  private RandomUtils _random;
  private Set<Integer> _intSet;
  private Set<String>  _stringSet;

  @Setup
  public void setUp(){
    _random = new RandomUtils();
    _intSet = ChronicleSetBuilder.of(Integer.class).entries(NUM_KEYS_PRELOAD).create();

    String str = StringUtils.repeat("a", 50);
    _stringSet = ChronicleSetBuilder.of(String.class).entries(NUM_KEYS_PRELOAD).averageKey(str).create();

    // populate sets for iterator and contains workloads
    for(int value=0;value<NUM_KEYS_PRELOAD;++value)
      _intSet.add(value);

    // populate sets for iterator and contains workloads
    _random.buildStringSetRandomRange(_stringSet,NUM_KEYS_PRELOAD,100);
  }



/*
    Workload: Int Sorted with no collision
    Number of entries : based on _gb of storage
   */

  @Benchmark
  public void insertIntSortedNoCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
    Set<Integer> set = ChronicleSetBuilder.of(Integer.class).entries(numEntries).create();

    int value;
    for(value=0;value<numEntries;++value)
      set.add(value);
  }

/*
    Workload: Int Sorted with collision
    Number of entries : based on _gb of storage
    Collision: COLLISION_FACTOR many collision per key
   */

  @Benchmark
  public void insertIntSortedCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
    Set<Integer> set = ChronicleSetBuilder.of(Integer.class).entries(numEntries/COLLISION_FACTOR).create();
    int maxValue = (int) numEntries/COLLISION_FACTOR;
    int value,count=0;

    for(value=0;value<=maxValue;++value) {
      while(count<(value*COLLISION_FACTOR) && count<numEntries) {
        set.add(value);
        ++count;
      }
    }
  }


/*
    Workload: Int Random with collision
    Number of entries : based on _gb of storage
    Collision: COLLISION_FACTOR many collision per key
   */

  @Benchmark
  public void insertIntRandomCollision(){
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
    Set<Integer> set = ChronicleSetBuilder.of(Integer.class).entries(numEntries/COLLISION_FACTOR).create();
    int maxValue = numEntries/COLLISION_FACTOR;
    _random.buildIntSetRandomRange(set,numEntries,maxValue);
  }


/*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 150 so 1/2^25 probability of collision
  */

  @Benchmark
  public void insertStringRandomLowCollision(){
    String str = StringUtils.repeat("a", _keyLength);
    int maxLength = 150;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2

    Set<String> set = ChronicleSetBuilder.of(String.class).averageKey(str).entries(numEntries).create();

    _random.buildStringSetRandomRange(set,numEntries,maxLength);
  }


/*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 20 for about 1:10 collision
  */

  @Benchmark
  public void insertStringRandomCollision(){
    String str = StringUtils.repeat("a", _keyLength);
    int maxLength = 20;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2

    Set<String> set = ChronicleSetBuilder.of(String.class).averageKey(str).entries(numEntries).create();

    _random.buildStringSetRandomRange(set,numEntries,maxLength);
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

    Set<byte[]> set = ChronicleSetBuilder.of(byte[].class).averageKey(new byte[_keyLength]).entries(numEntries).create();

    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);
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
    Set<byte[]> set = ChronicleSetBuilder.of(byte[].class).averageKey(new byte[_keyLength]).entries(numEntries).create();

    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);
  }


  // ITERATOR WORKLOADS //
  @Benchmark
  public void iterateInt(){
    Iterator<Integer> it = _intSet.iterator();
    while(it.hasNext()){
      it.next();
    }
  }

  @Benchmark
  public void iterateString(){
    Iterator<String> it = _stringSet.iterator();
    while(it.hasNext()){
      it.next();
    }
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
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkChronicleMap.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}

