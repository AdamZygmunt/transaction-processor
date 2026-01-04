package edu.prz.techbank.processor.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SparkConfig {

  private static final String JAVA_OPTIONS =
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
          + "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
          + "--add-opens=java.base/java.nio=ALL-UNNAMED "
          + "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
          + "--add-opens=java.base/java.util=ALL-UNNAMED "
          + "-Dlog4j.configurationFile=/opt/spark/conf/log4j2.properties";

  @Value("${spark.master}")
  private String sparkMaster;

  @Bean
  public SparkConf sparkConf() {
    return new SparkConf()
        .setAppName("TechBankTransactionProcessor")
        .setMaster(sparkMaster)
        .setJars(new String[]{"transaction-processor.jar"})
        .set("spark.driver.extraJavaOptions", JAVA_OPTIONS)
        .set("spark.executor.extraJavaOptions", JAVA_OPTIONS)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired", "false")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic");
  }

  @Bean
  public SparkSession sparkSession() {
    return SparkSession.builder()
        .config(sparkConf())
        .getOrCreate();
  }

}
