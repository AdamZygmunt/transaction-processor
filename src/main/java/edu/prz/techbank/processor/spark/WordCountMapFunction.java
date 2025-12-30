package edu.prz.techbank.processor.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.MapPartitionsFunction;

public class WordCountMapFunction implements MapPartitionsFunction<String, String> {

  @Override
  public Iterator<String> call(Iterator<String> it) {
    List<String> out = new ArrayList<>();
    while (it.hasNext()) {
      out.addAll(Arrays.asList(it.next().split(" ")));
    }
    return out.iterator();
  }
}
