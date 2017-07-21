package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadPkInfo extends AbstractSpannerFn<Void, Map<String, List<KeyPart>>> {

  private final SpannerConfig config;

  public ReadPkInfo(SpannerConfig config) {
    this.config = config;
  }
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    try (ReadOnlyTransaction tx =
        databaseClient().readOnlyTransaction()) {
      Map<String, List<KeyPart>> result = new HashMap<>();
      // Run a dummy sql statement to force the RPC and obtain the timestamp from the server.
      ResultSet resultSet = tx.executeQuery(Statement.newBuilder(
          "SELECT t.table_name, t.column_name, t.column_ordering  "
              + "FROM information_schema.index_columns AS t "
              + "WHERE t.index_name = 'PRIMARY_KEY' and t.table_catalog = '' AND t.table_schema "
              + "= ''  ORDER BY t.table_name, t.index_name, t.ordinal_position").build());
      while (resultSet.next()) {
        // do nothing
        String tableName = resultSet.getString(0);
        String columnName = resultSet.getString(1);
        String ordering = resultSet.getString(2);

        List<KeyPart> parts = result.get(tableName);
        if (parts == null) {
          parts = new ArrayList<>();
          result.put(tableName, parts);
        }

        parts.add(KeyPart.create(columnName, ordering.toUpperCase().equals("DESC")));
      }
      c.output(result);
    }
  }


  @Override
  SpannerConfig getSpannerConfig() {
    return config;
  }
}
