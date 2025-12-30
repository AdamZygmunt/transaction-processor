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

  private static final String ADD_OPENS_ =
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
          + "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
          + "--add-opens=java.base/java.nio=ALL-UNNAMED"
          + "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
          + "--add-opens=java.base/java.util=ALL-UNNAMED";

  @Value("${spark.master}")
  private String sparkMaster;

  @Bean
  public SparkConf sparkConf() {
    return new SparkConf()
        .set("spark.driver.extraJavaOptions", ADD_OPENS_)
        .set("spark.executor.extraJavaOptions", ADD_OPENS_)
//        .setJars(new String[]{"/path/to/jar/with/your/WordCountJob.jar"})
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrationRequired", "false")
        .setAppName("SpringBootSparkApp")
        .setMaster(sparkMaster);
  }

  @Bean
  public SparkSession sparkSession() {
    return SparkSession.builder()
        .config(sparkConf())
        .getOrCreate();
  }

}
