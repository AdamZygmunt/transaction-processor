package edu.prz.techbank.processor.spark;

import edu.prz.techbank.processor.domain.turnover.Turnover;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SparkTurnoverService {

  @Value("${hdfs.uri}")
  String hdfsUri;

  @Value("${processing.turnover.path}")
  String path;

  final SparkSession spark;

  public void writeTurnover(Dataset<Turnover> turnover) {
    turnover.write()
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .parquet(hdfsUri + path);
  }

  public Dataset<Turnover> getTurnover(String account) {
    return spark
        .read()
        .parquet(hdfsUri + path)
        .filter("account='" + account + "'")
//        .filter(col("account").equalTo(account) // alternatywa
        .as(Encoders.bean(Turnover.class));
  }

  public List<Turnover> getOrderedTurnoverAsList(String account) {
    return getTurnover(account)
        .orderBy("date")
        .collectAsList();
  }

}
