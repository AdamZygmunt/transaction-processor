package edu.prz.techbank.processor.spark;

import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SparkService {

  @Value("${hdfs.uri}")
  String hdfsUri;

  final SparkSession spark;

  public String runWordCount(String inputPath, String outputPath) {
    // Wczytaj plik z HDFS lub lokalnie
    Dataset<String> textFile = spark.read().textFile(hdfsUri + inputPath);

    // Rozbijanie na słowa i zliczanie
    Dataset<Row> wordCounts = textFile
        .flatMap(
            (FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(),
            org.apache.spark.sql.Encoders.STRING())
        .groupBy("value")
        .count();

    // Zapis do HDFS
    wordCounts.write().mode("overwrite").csv(hdfsUri + outputPath);

    return "Job Spark WordCount zakończony. Wyniki w: " + outputPath;
  }
}
