package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

/** Creates a batch transaction. */
class CreateTransactionFn extends AbstractSpannerFn<Object, Transaction> {

  private final SpannerIO.CreateTransaction config;

  CreateTransactionFn(SpannerIO.CreateTransaction config) {
    this.config = config;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    try (ReadOnlyTransaction readOnlyTransaction =
        databaseClient().readOnlyTransaction(config.getTimestampBound())) {
      ResultSet resultSet = readOnlyTransaction.executeQuery(Statement.of("SELECT 1"));
      while (resultSet.next()) {
        // do nothing
      }
      Transaction tx = Transaction.create(readOnlyTransaction.getReadTimestamp());
      c.output(tx);
    }
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return config.getSpannerConfig();
  }
}
