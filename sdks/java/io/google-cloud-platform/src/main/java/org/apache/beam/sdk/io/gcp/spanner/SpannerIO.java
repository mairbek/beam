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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.ServiceOptions;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AbortedException;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental {@link PTransform Transforms} for reading from and writing to <a
 * href="https://cloud.google.com/spanner">Google Cloud Spanner</a>.
 *
 * <h3>Reading from Cloud Spanner</h3>
 *
 * <p>To read from Cloud Spanner apply {@link SpannerIO.Read} transformation, that returns a
 * {@link PCollection} of {@link Struct Structs}. Here, each struct represents an individual row
 * returned from the read operation.
 * {@link SpannerIO.Read} supports both Query and Read APIs.
 *
 * <p>To run a <b>query</b>, specify a {@link SpannerIO.Read#withQuery} option. See the following
 * example:
 *
 * <pre>{@code
 *  PCollection<Struct> rows = p.apply(
 *      SpannerIO.read()
 *          .withInstanceId(instanceId)
 *          .withDatabaseId(dbId)
 *          .withQuery("SELECT name, email FROM users")
 *          .withTimestamp(Timestamp.now()));
 * }</pre>
 *
 * <p>To use the Read API, specify the table name, a list of columns and, optionally, a KeySet.
 * <pre>{@code
 *  PCollection<Struct> rows = p.apply(
 *      SpannerIO.read()
 *          .withInstanceId(instanceId)
 *          .withDatabaseId(dbId)
 *          .withTable(“users”)
 *          .withColumns(“name”, “email”)
 *          .withTimestampBound(TimestampBound.strong()));
 * }</pre>
 *
 * <p>To optimally read using index, specify the index name using withIndex option.
 *
 * <p>The pipeline is guaranteed to be executed on a consistent snapshot of data, utilizing the
 * power of read only transactions. You can use {@link SpannerIO
 * .Read#withTimestampBound}/{@link SpannerIO.Read#withTimestamp} to control staleness of the data.
 * To read more about Spanner read only transactions, follow the
 * <a href="https://cloud.google.com/spanner/docs/transactions#read-only_transactions">documentation
 * link</a>.
 *
 *
 * <h3>Writing to Cloud Spanner</h3>
 *
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
 *
 * <p>The default size of the batch is set to 1MB, to override this use {@link
 * Write#withBatchSizeBytes(long)}. Setting batch size to a small value or zero practically disables
 * batching.
 *
 * <p>The transform does not provide same transactional guarantees as Cloud Spanner. In particular,
 *
 * <ul>
 * <li>Mutations are not submitted atomically;
 * <li>A mutation is applied at least once;
 * <li>If the pipeline was unexpectedly stopped, mutations that were already applied will not get
 * rolled back.
 * </ul>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024 * 1024; // 1 MB

  /**
   * Creates an uninitialized instance of {@link Read}. Before use, the {@link Read} must be
   * configured with a {@link Read#withInstanceId} and {@link Read#withDatabaseId} that identify
   * the Cloud Spanner database.
   */
  @Experimental
  public static Read read() {
    return new AutoValue_SpannerIO_Read.Builder()
        .setTimestampBound(TimestampBound.strong())
        .setKeySet(KeySet.all())
        .build();
  }

  /**
   * Creates an uninitialized instance of {@link Write}. Before use, the {@link Write} must be
   * configured with a {@link Write#withInstanceId} and {@link Write#withDatabaseId} that identify
   * the Cloud Spanner database being written.
   */
  @Experimental
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder()
        .setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES)
        .build();
  }

  /**
   * A {@link PTransform} that reads data from Google Cloud Spanner.
   *
   * @see SpannerIO
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Struct>> {
    @Nullable
    abstract String getProjectId();

    @Nullable
    abstract String getInstanceId();

    @Nullable
    abstract String getDatabaseId();

    @Nullable
    abstract TimestampBound getTimestampBound();

    @Nullable
    abstract Statement getQuery();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract String getIndex();

    @Nullable
    abstract List<String> getColumns();

    @Nullable
    abstract KeySet getKeySet();

    abstract Builder toBuilder();

    @Nullable
    @VisibleForTesting
    abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

    public Read withTimestamp(Timestamp timestamp) {
      return withTimestampBound(TimestampBound.ofReadTimestamp(timestamp));
    }

    public Read withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    public Read withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    public Read withColumns(String... columns) {
      return withColumns(Arrays.asList(columns));
    }

    public Read withColumns(List<String> columns) {
      return toBuilder().setColumns(columns).build();
    }

    public Read withQuery(Statement statement) {
      return toBuilder().setQuery(statement).build();
    }

    public Read withQuery(String sql) {
      return withQuery(Statement.of(sql));
    }

    public Read withKeySet(KeySet keySet) {
      return toBuilder().setKeySet(keySet).build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setProjectId(String projectId);

      abstract Builder setInstanceId(String instanceId);

      abstract Builder setDatabaseId(String databaseId);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setQuery(Statement statement);

      abstract Builder setTable(String table);

      abstract Builder setIndex(String index);

      abstract Builder setColumns(List<String> columns);

      abstract Builder setKeySet(KeySet keySet);

      @VisibleForTesting
      abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

      abstract Read build();
    }

    SpannerOptions getSpannerOptions() {
      SpannerOptions.Builder builder = SpannerOptions.newBuilder();
      if (getServiceFactory() != null) {
        builder.setServiceFactory(getServiceFactory());
      }
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public Read withProjectId(String projectId) {
      return toBuilder().setProjectId(projectId).build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Read withInstanceId(String instanceId) {
      return toBuilder().setInstanceId(instanceId).build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * database.
     *
     * <p>Does not modify this object.
     */
    public Read withDatabaseId(String databaseId) {
      return toBuilder().setDatabaseId(databaseId).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(
          getInstanceId(),
          "SpannerIO.read() requires instance id to be set with withInstanceId method");
      checkNotNull(
          getDatabaseId(),
          "SpannerIO.read() requires database id to be set with withDatabaseId method");
      checkNotNull(
          getTimestampBound(),
          "SpannerIO.read() runs in a read only transaction and requires timestamp to be set "
              + "with withTimestampBound or withTimestamp method");

      if (getQuery() != null) {
        // TODO: validate query?
      } else if (getTable() != null) {
        // Assume read
        checkNotNull(
            getColumns(),
            "For a read operation SpannerIO.read() requires a list of "
                + "columns to set with withColumns method");
        checkArgument(
            !getColumns().isEmpty(),
            "For a read operation SpannerIO.read() requires a"
                + " list of columns to set with withColumns method");
      } else {
        throw new IllegalArgumentException(
            "SpannerIO.read() requires configuring query or read operation.");
      }
    }

    @Override
    public PCollection<Struct> expand(PBegin input) {
      return input
          .apply(Create.of(1))
          .apply("Execute query", ParDo.of(new SimpleSpannerReadFn(this))).setCoder
                      (new StructCoder());
    }
  }

  @VisibleForTesting
  static class SimpleSpannerReadFn extends DoFn<Object, Struct> {
    final Read config;
    transient Spanner service;
    transient DatabaseClient databaseClient;

    private SimpleSpannerReadFn(Read config) {
      this.config = config;
    }

    @Setup
    public void setup() throws Exception {
      SpannerOptions options = config.getSpannerOptions();
      service = options.getService();
      String projectId =
          config.getProjectId() == null
              ? ServiceOptions.getDefaultProjectId()
              : config.getProjectId();
      databaseClient =
          service.getDatabaseClient(
              DatabaseId.of(projectId, config.getInstanceId(), config.getDatabaseId()));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try (ReadOnlyTransaction readOnlyTransaction =
          databaseClient.readOnlyTransaction(config.getTimestampBound())) {
        ResultSet resultSet = execute(readOnlyTransaction);
        while (resultSet.next()) {
          c.output(resultSet.getCurrentRowAsStruct());
        }
      }
    }

    private ResultSet execute(ReadOnlyTransaction readOnlyTransaction) {
      if (config.getQuery() != null) {
        return readOnlyTransaction.executeQuery(config.getQuery());
      }
      if (config.getIndex() != null) {
        return readOnlyTransaction.readUsingIndex(
            config.getTable(), config.getIndex(), config.getKeySet(), config.getColumns());
      }
      return readOnlyTransaction.read(config.getTable(), config.getKeySet(), config.getColumns());
    }

    @Teardown
    public void tearDown() throws Exception {
      service.close();
    }
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
    abstract String getProjectId();

    @Nullable
    abstract String getInstanceId();

    @Nullable
    abstract String getDatabaseId();

    abstract long getBatchSizeBytes();

    @Nullable
    @VisibleForTesting
    abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setProjectId(String projectId);

      abstract Builder setInstanceId(String instanceId);

      abstract Builder setDatabaseId(String databaseId);

      abstract Builder setBatchSizeBytes(long batchSizeBytes);

      @VisibleForTesting
      abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

      abstract Write build();
    }

    SpannerOptions getSpannerOptions() {
      SpannerOptions.Builder builder = SpannerOptions.newBuilder();
      if (getServiceFactory() != null) {
        builder.setServiceFactory(getServiceFactory());
      }
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public Write withProjectId(String projectId) {
      return toBuilder().setProjectId(projectId).build();
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
    public Write withBatchSizeBytes(long batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
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

    @VisibleForTesting
    Write withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      return toBuilder().setServiceFactory(serviceFactory).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(
          getInstanceId(),
          "SpannerIO.write() requires instance id to be set with withInstanceId method");
      checkNotNull(
          getDatabaseId(),
          "SpannerIO.write() requires database id to be set with withDatabaseId method");
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input.apply("Write mutations to Cloud Spanner", ParDo.of(new SpannerWriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("instanceId", getInstanceId()).withLabel("Output Instance"))
          .addIfNotNull(
              DisplayData.item("databaseId", getDatabaseId()).withLabel("Output Database"));
    }
  }

  /** Batches together and writes mutations to Google Cloud Spanner. */
  @VisibleForTesting
  static class SpannerWriteFn extends DoFn<Mutation, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteFn.class);
    private final Write spec;
    private transient Spanner spanner;
    private transient DatabaseClient dbClient;
    // Current batch of mutations to be written.
    private List<Mutation> mutations;
    private long batchSizeBytes = 0;

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES)
            .withInitialBackoff(Duration.standardSeconds(5));

    @VisibleForTesting
    SpannerWriteFn(Write spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() throws Exception {
      spanner = spec.getSpannerOptions().getService();
      dbClient =
          spanner.getDatabaseClient(
              DatabaseId.of(projectId(), spec.getInstanceId(), spec.getDatabaseId()));
      mutations = new ArrayList<>();
      batchSizeBytes = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation m = c.element();
      mutations.add(m);
      batchSizeBytes += MutationSizeEstimator.sizeOf(m);
      if (batchSizeBytes >= spec.getBatchSizeBytes()) {
        flushBatch();
      }
    }

    private String projectId() {
      return spec.getProjectId() == null
          ? ServiceOptions.getDefaultProjectId()
          : spec.getProjectId();
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
      spanner.close();
    }

    /**
     * Writes a batch of mutations to Cloud Spanner.
     *
     * <p>If a commit fails, it will be retried up to {@link #MAX_RETRIES} times. If the retry limit
     * is exceeded, the last exception from Cloud Spanner will be thrown.
     *
     * @throws AbortedException if the commit fails or IOException or InterruptedException if
     *     backing off between retries fails.
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
          LOG.error(
              "Error writing to Spanner ({}): {}", exception.getCode(), exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      LOG.debug("Successfully wrote {} mutations", mutations.size());
      mutations = new ArrayList<>();
      batchSizeBytes = 0;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", spec.getInstanceId()).withLabel("Instance"))
          .addIfNotNull(DisplayData.item("databaseId", spec.getDatabaseId()).withLabel("Database"));
    }
  }

  private SpannerIO() {} // Prevent construction.
}
