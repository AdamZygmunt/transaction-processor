package edu.prz.techbank.processor.parquet;

import edu.prz.techbank.processor.domain.Transaction;
import edu.prz.techbank.processor.exception.GeneralModuleException;
import edu.prz.techbank.processor.hdfs.HdfsFileUtils;
import edu.prz.techbank.processor.hdfs.HdfsFileUtils.FileType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetTransactionService {

  final org.apache.hadoop.conf.Configuration hadoopConfig;

  @Value("${parquet.max-records}")
  private int maxRecords;

  @Value("${parquet.compression-codec}")
  CompressionCodecName defaultCompressionCodec;

  private List<Transaction> buffer = createBuffer();

  static final String TRANSACTION_SCHEMA = """
      message transaction {
        required binary id (UTF8);
        required binary sender (UTF8);
        required binary beneficiary (UTF8);
        optional double amount;
        required binary currency (UTF8);
        required int64 timestamp;
      }
      """;

  static final MessageType schema = MessageTypeParser.parseMessageType(TRANSACTION_SCHEMA);
  static final SimpleGroupFactory factory = new SimpleGroupFactory(schema);

  public void writeTransactions(String directory, List<Transaction> transactions) {

    Path hdfsPath = HdfsFileUtils.generateNewFilePath(directory, FileType.PARQUET);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(
            HadoopOutputFile.fromPath(hdfsPath, hadoopConfig))
        .withConf(hadoopConfig)
        .withType(schema)
        .withCompressionCodec(defaultCompressionCodec)
        .build()) {

      for (Transaction t : transactions) {

        writer.write(factory.newGroup()
            .append("id", t.getId())
            .append("sender", t.getSender())
            .append("beneficiary", t.getBeneficiary())
            .append("amount", t.getAmount().doubleValue())
            .append("currency", t.getCurrency())
            .append("timestamp", t.getTimestamp()));
      }
    } catch (IOException e) {
      throw new GeneralModuleException("Transactions writing error", e);
    }
  }

  public void writeTransactionsUsingBuffer(String directory, List<Transaction> transactions) {

    if (buffer.size() + transactions.size() > maxRecords) {
      log.info("Max records reached. Doing rotation.");
      val bufferToWrite = buffer;
      this.buffer = createBuffer();
      writeTransactions(directory, bufferToWrite);
    }

    buffer.addAll(transactions);
  }

  private List<Transaction> createBuffer() {
    return Collections.synchronizedList(new ArrayList<>());
  }

}
