package org.example;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;


// 100mb to 5gb increments of 500mb
public class MemoryUsage {

  private int ROUNDS = 5;
  private double[] _gb = {0.001,0.005,0.05,0.5,1.0};

  private int[] _numIntKeys;

  private int[] _numVarKeys;

  private int _avgKeyLen = 120;

  private Set<String> stringSetKeys;

  private Set[] set;
  private
  void init(){
    int GB_TO_BYTES = 1024*1024*1024;
    _numIntKeys = new int[ROUNDS];
    _numVarKeys = new int[ROUNDS];

    for(int i=0;i<ROUNDS;i++) {
      _numIntKeys[i] = (int) (GB_TO_BYTES * _gb[i]) / 4;
      _numVarKeys[i] = (int) (GB_TO_BYTES * _gb[i]) / _avgKeyLen;
    }
    RandomUtils random = new RandomUtils();
    //stringSetKeys = new HashSet<>(_numVarKeys[ROUNDS-1]);
    //random.buildStringSetRandomRangeExact(stringSetKeys,_numVarKeys[ROUNDS-1],_avgKeyLen); // only 1GB worth of keys
    set = new Set[ROUNDS];
  }

  private static String toGB(long init) {
    return (Long.valueOf(init).doubleValue() / (1024 * 1024 * 1024)) + " GB";
  }

  private static String toMB(long init) {
    return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
  }

  public void printMemoryChronicle(ChronicleSet set) throws InterruptedException {
    System.gc();
    Thread.sleep(30000);
    System.out.println("Heap Memory: " + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory
    System.out.println("OffHeap Memory for Chronicle Set " + toMB(set.offHeapMemoryUsed())); //offHeapMemory
  }

  public void printMemoryMapDB() throws InterruptedException {
    System.gc();
    Thread.sleep(30000);
    System.out.println("Heap Memory: " + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory
    System.out.println("OffHeap Memory for Map DB "); //offHeapMemory

    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (BufferPoolMXBean pool : pools) {
      System.out.println(pool.getName());
      System.out.println(pool.getCount());
      System.out.println("memory used " + pool.getMemoryUsed() + " mb: " + toMB(pool.getMemoryUsed()));
      System.out.println("total capacity" + pool.getMemoryUsed() + " mb: " +  toMB(pool.getTotalCapacity()));
      System.out.println();
    }
  }

 public void checkMemoryUsageMapDBInt() throws InterruptedException {
   DB db = DBMaker.memoryDirectDB().make();
    for(int i=4;i<ROUNDS;i++) {
      System.out.println("Heap Memory: " + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory

      String s = "int" + i;

      System.out.println("*****************Iteration " + i + " Keys " +_numIntKeys[i] + " GB " + _gb[i] + "*************************");
      printMemoryMapDB();

      set[i]= db.hashSet(s).serializer(Serializer.INTEGER).createOrOpen();

      int value;
      for (value = 0; value < _numIntKeys[i]; ++value)
        set[i].add(value);
      System.out.println("---- After Loading: -----");
      //System.out.println("Size of set: " + set[i].size());

      printMemoryMapDB();
    }
  }

  public void checkMemoryUsageMapDBString() throws InterruptedException {
    DB db = DBMaker.memoryDirectDB().make();
    RandomUtils random = new RandomUtils();
    printMemoryMapDB();
    for(int i=0;i<ROUNDS;i++) {
      //System.out.println("Heap Memory: " + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory

      String s = "s" + i;
      System.out.println("**************************Iteration " + i + " Keys " +_numVarKeys[i] + " GB " + _gb[i] + "*******************************");
      printMemoryMapDB();

      set[i]= db.hashSet(s).serializer(Serializer.STRING).createOrOpen();
      //printMemoryMapDB();

      //random.buildStringSetRandomRangeExact(set,_numVarKeys[i],_avgKeyLen*2);
      random.CopyFromSet(stringSetKeys,set[i],_numVarKeys[i]);

      System.out.println("---- After Loading: -----");
      printMemoryMapDB();
      //System.out.println("Size of set: "+set[i].size());
    }
  }

  public void checkMemoryUsageChronicleInt() throws InterruptedException {
    for(int i=0;i<ROUNDS;i++) {
      System.gc();
      Thread.sleep(30000);
      System.out.println("Initial Heap Memory:" +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));

      set[i] = ChronicleSetBuilder.of(Integer.class).entries(_numIntKeys[i]).create();

      System.out.println("*******************Iteration " + i + " Keys " +_numIntKeys[i] + " GB " + _gb[i] + "**********************");

      int value;
      for (value = 0; value < _numIntKeys[i]; ++value)
        set[i].add(value);

      System.out.println("---- After Loading: -----");
      System.out.println("Size of set: " + set[i].size());

      printMemoryChronicle((ChronicleSet) set[i]);
    }
  }

  public void checkMemoryUsageChronicleString() throws InterruptedException {
    RandomUtils random = new RandomUtils();
    System.out.println("Heap Memory: " + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory

    String str = StringUtils.repeat("a", _avgKeyLen);

    for(int i=0;i<ROUNDS;i++) {
      System.gc();
      Thread.sleep(30000);
      System.out.println("Initial Heap Memory:" +  toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));


      set[i] = ChronicleSetBuilder.of(String.class).averageKey(str).entries(_numVarKeys[i]).create();

      System.out.println("*******************Iteration " + i + " Keys " +_numVarKeys[i] + " GB " + _gb[i] + "**********************");
      //printMemoryChronicle((ChronicleSet) set[i]);
      System.out.println("---- After Loading: -----");
      random.CopyFromSet(stringSetKeys,set[i],_numVarKeys[i]);
      System.out.println("Size of set: "+set[i].size());

      printMemoryChronicle((ChronicleSet) set[i]);
    }
  }

  // so that the sets created are not garbage collected
  void callToPreventGC(){
    for(int i=0;i<ROUNDS;i++)
      set[i].size();
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Initial Heap Memory: " + toMB(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())); //heap memory
    MemoryUsage m = new MemoryUsage();
    m.init();
    //m.checkMemoryUsageChronicleInt();
    //m.checkMemoryUsageChronicleString();
    m.checkMemoryUsageMapDBInt();
    //m.checkMemoryUsageMapDBString();
    m.callToPreventGC();
  }

}
