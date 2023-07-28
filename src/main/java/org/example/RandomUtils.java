package org.example;

import org.apache.commons.lang3.RandomStringUtils;
import java.util.Random;
import java.util.Set;


public class RandomUtils {
  private final Random _random = new Random();

  // The generateRandomString method returns a random alphabetic string of length at most maxLength
  public String generateRandomString(int minLength, int maxLength) {

    return RandomStringUtils.randomAlphabetic(minLength,maxLength+1);
  }

  // The generateRandomByteArray method returns a random byte array of length at most maxLength
  public byte[] generateRandomByteArray(int maxLength) {

    // pick a random length which is at most maxLength
    int length = _random.nextInt(maxLength) + 1; // Adding 1 to avoid length of 0

    byte[] byteArray = new byte[length];
    _random.nextBytes(byteArray);

    return byteArray;
  }

  public void buildStringSetRandomRange(Set<String> set, long numEntries, int maxLength){
    for(int i=0;i<numEntries;++i){
      set.add(generateRandomString(1,maxLength));
    }
  }

  public void buildByteArraySetRandomRange(Set<byte[]> set, long numEntries, int maxLength){
    for(int i=0;i<numEntries;++i){
      set.add(generateRandomByteArray(maxLength));
    }
  }

  public void buildIntSetRandomRange(Set<Integer> set, long numEntries, int maxValue){
    for(int i=0;i<numEntries;++i){
      set.add(_random.nextInt(maxValue));
    }
  }

  public int getRandomInt(int minValue,int maxValue){
    return _random.nextInt(maxValue-minValue)+minValue;
  }
}


