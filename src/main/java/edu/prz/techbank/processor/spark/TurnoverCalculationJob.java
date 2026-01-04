package edu.prz.techbank.processor.spark;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import edu.prz.techbank.processor.domain.turnover.Turnover;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.spark.sql.Encoders;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public final class TurnoverCalculationJob {

  final SparkTransactionService transactionService;
  final SparkTurnoverService turnoverService;

  public void run(LocalDate date) {

    val turnover = transactionService.getTransactions(date)
        .groupBy("sender", "currency")
        .agg(
            sum("amount").alias("amount")
        )
        .withColumnRenamed("sender", "account")
        .withColumn("date", lit(date))
        .as(Encoders.bean(Turnover.class));

    turnoverService.writeTurnover(turnover);
  }

}
