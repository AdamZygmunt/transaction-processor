package edu.prz.techbank.processor.spark;

import edu.prz.techbank.processor.domain.transaction.Transaction;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SparkTransactionService {

  @Value("${hdfs.uri}")
  String hdfsUri;

  @Value("${processing.transaction.path}")
  String path;

  final SparkSession spark;

  public Dataset<Transaction> getTransactions(LocalDate date) {
    return spark
        .read()
        .parquet(hdfsUri + path + "/date=" + date)
        .as(Encoders.bean(Transaction.class));
  }

  public List<Transaction> getTransactionsAsList(LocalDate date) {
    return getTransactions(date)
        .collectAsList();
  }

}
