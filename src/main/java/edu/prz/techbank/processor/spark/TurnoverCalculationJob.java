package edu.prz.techbank.processor.spark;

import edu.prz.techbank.processor.domain.transaction.Transaction;
import edu.prz.techbank.processor.domain.turnover.Turnover;
import edu.prz.techbank.processor.domain.turnover.TurnoverCalculator;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public final class TurnoverCalculationJob {

  @Value("${hdfs.uri}")
  String hdfsUri;

  final SparkSession spark;

  public void run(String inputPath, LocalDate date, String outputPath) {
    val turnover = getTransactions(inputPath, date)
        .map(new TurnoverCalculator(), Encoders.bean(Turnover.class));
    writeTurnover(turnover, outputPath);
  }

  private Dataset<Transaction> getTransactions(String inputPath, LocalDate date) {
    return spark
        .read()
        .parquet(hdfsUri + inputPath + "/date=" + date)
        .as(Encoders.bean(Transaction.class));
  }

  private void writeTurnover(Dataset<Turnover> turnover, String outputPath) {
    turnover.write()
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .parquet(hdfsUri + outputPath);
  }

  public List<Transaction> getTransactionsAsList(String inputPath, LocalDate date) {
    return getTransactions(inputPath, date)
        .collectAsList();
  }

}
