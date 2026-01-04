package edu.prz.techbank.processor.domain.turnover;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Turnover {

  String account;
  String date; // yyyy-MM-dd
  Double amount;
}
