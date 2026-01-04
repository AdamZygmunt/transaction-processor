package edu.prz.techbank.processor.spark;

import edu.prz.techbank.processor.domain.transaction.Transaction;
import java.time.LocalDate;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/processing")
@Slf4j
public class SparkProcessingController {

  final TurnoverCalculationJob turnoverCalculationJob;

  @PostMapping("/turnover")
  public ResponseEntity<String> runTurnoversCalculation(
      @RequestParam String inputPath,
      @RequestParam LocalDate date,
      @RequestParam String outputPath) {

    turnoverCalculationJob.run(inputPath, date, outputPath);

    return ResponseEntity.ok().build();
  }

  @GetMapping("/transactions")
  public ResponseEntity<List<Transaction>> getTransactions(
      @RequestParam String inputPath,
      @RequestParam LocalDate date) {

    return ResponseEntity.ok(turnoverCalculationJob.getTransactionsAsList(inputPath, date));
  }
}
