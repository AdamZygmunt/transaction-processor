package edu.prz.techbank.processor.domain;

import java.math.BigDecimal;
import lombok.Getter;

@Getter
public class Transaction {

  String id;
  String sender;
  String beneficiary;
  BigDecimal amount;
  String currency;
  Long timestamp;
}
