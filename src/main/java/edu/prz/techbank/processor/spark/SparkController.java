package edu.prz.techbank.processor.spark;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/spark")
@Slf4j
public class SparkController {

  final SparkService sparkService;

  @PostMapping("/wordcount")
  public ResponseEntity<String> runWordCount(@RequestParam String inputPath,
      @RequestParam String outputPath) {
    try {
      String result = sparkService.runWordCount(inputPath, outputPath);
      return ResponseEntity.ok(result);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return ResponseEntity.internalServerError()
          .body("Błąd uruchamiania joba Spark: " + e.getMessage());
    }
  }
}
