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
import com.google.cloud.spanner.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
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
 * <p>This functionality is not yet implemented.
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
 * Write#withBatchSize(long)}. Setting batch size to a small value or zero practically disables
 * batching.
 *
 * <p>The transform does not provide same transactional guarantees as Cloud Spanner. In particular,
 * <li>Mutations are not submitted atomically;
 * <li>A mutation is applied at least once;
 * <li>If the pipeline was unexpectedly stopped, mutations that were already applied will not get
 *     rolled back.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private static final long DEFAULT_BATCH_SIZE = 1024 * 1024; // 1 MB

  /**
   * Creates an uninitialized instance of {@link Write}. Before use, the {@link Write} must be
   * configured with a {@link Write#withInstanceId} and {@link Write#withDatabaseId} that identify
   * the Cloud Spanner database being written.
   */
  @Experimental
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder().setBatchSize(DEFAULT_BATCH_SIZE).build();
  }

  @Experimental
  public static Read read() {
    return new AutoValue_SpannerIO_Read.Builder().build();
  }

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
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public Read withProjectId(String projectId) {
      return toBuilder().setProjectId(projectId).build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Read withInstanceId(String instanceId) {
      return toBuilder().setInstanceId(instanceId).build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
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
          // TODO(mairbek): validate query?
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
            "SpannerIO.read() requires configuring query or read " + "operation.");
      }
    }

    @Override
    public PCollection<Struct> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(new SpannerSource(this)));
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

    abstract long getBatchSize();

    @Nullable
    @VisibleForTesting
    abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setProjectId(String projectId);

      abstract Builder setInstanceId(String instanceId);

      abstract Builder setDatabaseId(String databaseId);

      abstract Builder setBatchSize(long batchSize);

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
    private long batchSize = 0;

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
      batchSize = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation m = c.element();
      mutations.add(m);
      batchSize += MutationSizeEstimator.sizeOf(m);
      if (batchSize >= spec.getBatchSize()) {
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
      spanner.closeAsync().get();
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
      batchSize = 0;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", spec.getInstanceId()).withLabel("Instance"))
          .addIfNotNull(DisplayData.item("databaseId", spec.getDatabaseId()).withLabel("Database"));
    }
  }

  private static class SpannerSource extends BoundedSource<Struct> {
    private final Read config;

    private SpannerSource(Read config) {
      this.config = config;
    }

    @Override
    public List<? extends BoundedSource<Struct>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // TODO(mairbek): liquid sharding is not supported yet.
      return ImmutableList.of(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0L;
    }

    @Override
    public BoundedReader<Struct> createReader(PipelineOptions options) throws IOException {
      if (config.getQuery() != null) {
        return new SpannerQueryReader(this, config);
      } else if (config.getIndex() != null) {
        return new SpannerIndexReader(this, config);
      }
      return new SpannerReadReader(this, config);
    }

    @Override
    public void validate() {}

    @Override
    public Coder<Struct> getDefaultOutputCoder() {
      return new StructCoder();
    }
  }

  private abstract static class BaseSpannerReader extends BoundedSource.BoundedReader<Struct> {
    final SpannerSource source;
    final Read config;

    ResultSet resultSet;
    Spanner service;
    DatabaseClient databaseClient;
    ReadOnlyTransaction readOnlyTransaction;

    BaseSpannerReader(SpannerIO.Read config, SpannerIO.SpannerSource source) {
      this.config = config;
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      SpannerOptions options = config.getSpannerOptions();
      service = options.getService();
      databaseClient =
          service.getDatabaseClient(
              DatabaseId.of(
                  options.getProjectId(), config.getInstanceId(), config.getDatabaseId()));
      readOnlyTransaction = databaseClient.readOnlyTransaction(config.getTimestampBound());
      startRead();
      return true;
    }

    protected abstract void startRead();

    @Override
    public boolean advance() throws IOException {
      return resultSet.next();
    }

    @Override
    public Struct getCurrent() throws NoSuchElementException {
      return resultSet.getCurrentRowAsStruct();
    }

    @Override
    public void close() throws IOException {
      resultSet.close();
      readOnlyTransaction.close();
      try {
        service.closeAsync().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException("Cannot properly shut down spanner", e);
      }
    }

    @Override
    public BoundedSource<Struct> getCurrentSource() {
      return source;
    }
  }

  @VisibleForTesting
  static class SpannerQueryReader extends BaseSpannerReader {

    SpannerQueryReader(SpannerSource source, Read config) {
      super(config, source);
    }

    protected void startRead() {
      resultSet = readOnlyTransaction.executeQuery(config.getQuery());
    }
  }

  @VisibleForTesting
  static class SpannerReadReader extends BaseSpannerReader {

    SpannerReadReader(SpannerSource source, Read config) {
      super(config, source);
    }

    protected void startRead() {
      resultSet =
          readOnlyTransaction.read(config.getTable(), config.getKeySet(), config.getColumns());
    }
  }

  @VisibleForTesting
  static class SpannerIndexReader extends BaseSpannerReader {

    SpannerIndexReader(SpannerSource source, Read config) {
      super(config, source);
    }

    protected void startRead() {
      resultSet =
          readOnlyTransaction.readUsingIndex(
              config.getTable(), config.getIndex(), config.getKeySet(), config.getColumns());
    }
  }

  private SpannerIO() {} // Prevent construction.
}
