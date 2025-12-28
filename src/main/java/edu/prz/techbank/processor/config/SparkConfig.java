package edu.prz.techbank.processor.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

  @Value("${spark.master}")
  private String sparkMaster;

  @Bean
  public SparkSession sparkSession() {
    return SparkSession.builder()
        .appName("SpringBootSparkApp")
        .master(sparkMaster) // spark://spark-master:7077
        .getOrCreate();
  }
}
