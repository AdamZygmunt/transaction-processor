package edu.prz.techbank.processor.domain.turnover;

import edu.prz.techbank.processor.domain.transaction.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;

@Slf4j
public class TurnoverCalculator implements MapFunction<Transaction, Turnover> {

  @Override
  public Turnover call(Transaction transaction) {
    return new Turnover(transaction.getSender(), transaction.getDate(), 0.0); // FIXME
  }

}
