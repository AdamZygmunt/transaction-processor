package edu.prz.techbank.processor.spark;

import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;

@Service
public final class WordCountJob {

  private WordCountJob() {
  }

  public void run(
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
