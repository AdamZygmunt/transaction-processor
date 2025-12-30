package edu.prz.techbank.processor.spark;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class SparkService {

  @Value("${hdfs.uri}")
  String hdfsUri;

  final SparkSession spark;

  public String runWordCount(String inputPath, String outputPath) {

    WordCountJob.run(spark, hdfsUri + inputPath, hdfsUri + outputPath);

    return "Job Spark WordCount zakończony. Wyniki w: " + hdfsUri + outputPath;
  }

}
