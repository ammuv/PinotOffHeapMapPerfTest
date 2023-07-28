package org.example;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  Baseline Benchmark HashSet in Java Collections for all our Workloads
 */

@BenchmarkMode({Mode.AverageTime,Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 30)
@Measurement(iterations = 3, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkSetBaseline {
  private static final int NUM_KEYS_PRELOAD = 1000000;
  private static final int GB_TO_BYTES = 1024*1024*1024; // conversion constant
  private static final int COLLISION_FACTOR = 100;  //about COLLISION_FACTOR many collisions per key for collision workloads
  @Param({"0.005","0.05"}) //GB of data to store
  float _gb;

  private RandomUtils _random;
  private Set<Integer> _intSet;
  private Set<String>  _stringSet;

  @Setup
  public void setUp(){
    _random = new RandomUtils();
    _intSet = new HashSet<>(NUM_KEYS_PRELOAD);
    _stringSet = new HashSet<>(NUM_KEYS_PRELOAD);

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
    Set<Integer> set = new HashSet<>();
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;

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
    Set<Integer> set = new HashSet<>();
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
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
    Set<Integer> set = new HashSet<>();
    int numEntries = (int)(GB_TO_BYTES*_gb)/4;
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
    Set<String> set = new HashSet<>();
    int maxLength = 150;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2
    _random.buildStringSetRandomRange(set,numEntries,maxLength);
  }

  /*
   Workload: Random String with minimum collision
   Number of entries : based on _gb of storage
   Collision: maxLength set to 20 for about 1:10 collision
  */
  @Benchmark
  public void insertStringRandomCollision(){
    Set<String> set = new HashSet<>();
    int maxLength = 20;
    int numEntries = (int)(GB_TO_BYTES*_gb)/maxLength; // char in Java is 2 bytes and average length is maxLength/2
    _random.buildStringSetRandomRange(set,numEntries,maxLength);
  }

  /*
  Workload: Random Byte Array with minimum collision
  Number of entries : based on _gb of storage
  Collision: maxLength set to 20 for about 1:10 collision
  */
  @Benchmark
  public void insertByteArrayRandomLowCollision(){
    Set<byte[]> set = new HashSet<>();
    int maxLength = 120;
    int numEntries = (int)(GB_TO_BYTES*_gb*2)/maxLength;  // average length is maxLength/2
    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);
  }

  /*
  Workload: Random Byte Array with collision
  Number of entries : based on _gb of storage
  Collision: maxLength set to 10 for about 1:10 collision
  */
  @Benchmark
  public void insertByteArrayRandomCollision(){
    Set<byte[]> set = new HashSet<>();
    int maxLength = 10;
    int numEntries = (int)(GB_TO_BYTES*_gb*2)/maxLength;  // average length is maxLength/2
    _random.buildByteArraySetRandomRange(set,numEntries,maxLength);
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
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkSetBaseline.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
