/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental {@link PTransform Transforms} for reading from and writing to
 * <a href="https://cloud.google.com/spanner">Google Cloud Spanner</a>.
 *
 * <h3>Reading from Cloud Spanner</h3>
 * <p>This functionality is not yet implemented.
 *
 * <h3>Writing to Cloud Spanner</h3>
 * <p>The Cloud Spanner {@link SpannerIO.Write} transform writes to Cloud Spanner by executing a
 * collection of input row {@link Mutation Mutations}. The mutations grouped into batches for
 * efficiency.
 *
 * <p>To configure the write transform, create an instance using {@link #write()} and then specify
 * the destination Cloud Spanner instance ({@link Write#withInstanceId(String)} and destination
 * database ({@link Write#withDatabaseId(String)}). For example:
 *
 * <pre>{@code
 * // Earlier in the pipeline, create a PCollection of Mutations to be written to Cloud Spanner.
 * PCollection<Mutation> mutations = ...;
 * // Write mutations.
 * mutations.apply(
 *     "Write", SpannerIO.write().withInstanceId("instance").withDatabaseId("database"));
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private static final long DEFAULT_BATCH_SIZE = 1024 * 1024;  // 1 MB

  /**
   * Creates an uninitialized instance of {@link Write}. Before use, the {@link Write} must be
   * configured with a {@link Write#withInstanceId} and {@link Write#withDatabaseId} that identify
   * the Cloud Spanner database being written.
   */
  @Experimental
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder().setBatchSize(DEFAULT_BATCH_SIZE).build();
  }

  /**
   * A {@link PTransform} that writes {@link Mutation} objects to Google Cloud Spanner.
   *
   * @see SpannerIO
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Mutation>, PDone> {

    @Nullable
    abstract String getInstanceId();

    @Nullable
    abstract String getDatabaseId();

    abstract Builder toBuilder();

    public abstract long batchSize();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setInstanceId(String instanceId);

      abstract Builder setDatabaseId(String databaseId);

      abstract Builder setBatchSize(long batchSize);

      abstract Write build();
    }

    // TODO(mairbek): Once SpannerOptions is serializable, make it Write parameter.
    public SpannerOptions spannerOptions() {
      SpannerOptions.Builder builder = SpannerOptions.newBuilder();
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Write withInstanceId(String instanceId) {
      return toBuilder().setInstanceId(instanceId).build();
    }

   /**
     * Returns a new {@link SpannerIO.Write} with a new batch size limit.
     *
     * <p>Does not modify this object.
     */
    public Write withBatchSize(long batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * database.
     *
     * <p>Does not modify this object.
     */
    public Write withDatabaseId(String databaseId) {
      return toBuilder().setDatabaseId(databaseId).build();
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input.apply("Write mutations to Cloud Spanner", ParDo.of(
           new SpannerWriterFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", getInstanceId())
              .withLabel("Output Instance"))
          .addIfNotNull(DisplayData.item("databaseId", getDatabaseId())
              .withLabel("Output Database"));
    }
  }

  /**
   * {@link DoFn} that writes {@link Mutation}s to Google Cloud Spanner. Mutations are written in
   * batches, where the maximum batch size is {@link SpannerIO#DEFAULT_BATCH_SIZE}.
   *
   * <p>Commits are non-transactional.  If a commit fails, it will be retried (up to
   * {@link SpannerWriterFn#MAX_RETRIES} times). This means that the mutation operation should be
   * idempotent.
   *
   * <p>See <a href="https://cloud.google.com/spanner">Google Cloud Spanner documentation</a>.
   */
  @VisibleForTesting
  static class SpannerWriterFn extends DoFn<Mutation, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterFn.class);
    private final Write spec;
    private transient Spanner spanner;
    private transient DatabaseClient dbClient;
    // Current batch of mutations to be written.
    private final List<Mutation> mutations = new ArrayList<>();
    private long batchSize = 0;

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));

    @VisibleForTesting
    SpannerWriterFn(Write spec) {
        this.spec = spec;
    }

    @Setup
    public void setup() throws Exception {
      spanner = spec.spannerOptions().getService();
      String projectId = spec.spannerOptions().getProjectId();
      dbClient = spanner.getDatabaseClient(
          DatabaseId.of(projectId, spec.getInstanceId(), spec.getDatabaseId()));
      mutations.clear();
      batchSize = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation m = c.element();
      mutations.add(m);
      if (batchSize >= spec.batchSize()) {
        flushBatch();
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (!mutations.isEmpty()) {
        flushBatch();
      }
    }

    @Teardown
    public void teardown() throws Exception {
      if (spanner == null) {
        return;
      }
      spanner.closeAsync().get();
    }

    /**
     * Writes a batch of mutations to Cloud Spanner.
     *
     * <p>If a commit fails, it will be retried up to {@link #MAX_RETRIES} times.
     * If the retry limit is exceeded, the last exception from Cloud Spanner will be
     * thrown.
     *
     * @throws AbortedException if the commit fails or IOException or InterruptedException if
     * backing off between retries fails.
     */
    private void flushBatch() throws AbortedException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} mutations", mutations.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

      while (true) {
        // Batch upsert rows.
        try {
          dbClient.writeAtLeastOnce(mutations);

          // Break if the commit threw no exception.
          break;
        } catch (AbortedException exception) {
          // Only log the code and message for potentially-transient errors. The entire exception
          // will be propagated upon the last retry.
          LOG.error("Error writing to Spanner ({}): {}", exception.getCode(),
              exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      LOG.debug("Successfully wrote {} mutations", mutations.size());
      mutations.clear();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", spec.getInstanceId())
              .withLabel("Instance"))
          .addIfNotNull(DisplayData.item("databaseId", spec.getDatabaseId())
              .withLabel("Database"));
    }
  }

  private SpannerIO() {} // Prevent construction.

}
