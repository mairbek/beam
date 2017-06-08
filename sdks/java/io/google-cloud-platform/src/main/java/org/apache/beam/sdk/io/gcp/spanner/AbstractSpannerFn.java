package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Abstract {@link DoFn} that manages {@link Spanner} lifecycle. Use {@link
 * AbstractSpannerFn#databaseClient} to access the Cloud Spanner database client.
 */
abstract class AbstractSpannerFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
  private transient Spanner spanner;
  private transient DatabaseClient databaseClient;

  abstract SpannerConfig getSpannerConfig();

  @Setup
  public void setup() throws Exception {
    SpannerConfig spannerConfig = getSpannerConfig();
    SpannerOptions options = spannerConfig.getSpannerOptions();
    spanner = options.getService();
    String projectId =
        spannerConfig.getProjectId() == null
            ? ServiceOptions.getDefaultProjectId()
            : spannerConfig.getProjectId();
    databaseClient =
        spanner.getDatabaseClient(
            DatabaseId.of(projectId, spannerConfig.getInstanceId(), spannerConfig.getDatabaseId()));
  }

  @Teardown
  public void teardown() throws Exception {
    if (spanner == null) {
      return;
    }
    spanner.close();
    spanner = null;
  }

  protected DatabaseClient databaseClient() {
    return databaseClient;
  }
}
