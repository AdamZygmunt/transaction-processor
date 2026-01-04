package edu.prz.techbank.processor.domain.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

  String id;
  String sender;
  String beneficiary;
  Double amount;
  String currency;
  String date; // yyyy-MM-dd
  Long timestamp;
}
