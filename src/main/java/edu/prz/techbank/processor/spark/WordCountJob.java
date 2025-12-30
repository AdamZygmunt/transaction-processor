package edu.prz.techbank.processor.spark;

import java.io.Serializable;
import org.apache.spark.sql.*;

public final class WordCountJob implements Serializable {

  private WordCountJob() {
  }

  public static void run(
      SparkSession spark,
      String inputPath,
      String outputPath) {

    Dataset<String> textFile = spark.read().textFile(inputPath);

//    Dataset<Row> wordCounts = textFile.groupBy("value").count();
//
//    wordCounts.write()
//        .mode(SaveMode.Overwrite)
//        .csv(outputPath);

    Dataset<String> words = textFile.mapPartitions(
        new WordCountMapFunction(),
        Encoders.STRING()
    );

    Dataset<Row> wordCounts = words.groupBy("value").count();

    wordCounts.write()
        .mode(SaveMode.Overwrite)
        .csv(outputPath);
  }
}
