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
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

/**
 * Use {@link SpannerIO} to read from and write to
 * <a href="https://cloud.google.com/spanner">Google Cloud Spanner</a>.
 *
 * <h3>Reading from Cloud Spanner</h3>
 *
 * <p>To read from Cloud Spanner, apply {@link SpannerIO.Read} transformation. It will return a
 * {@link PCollection} of {@link Struct Structs}, where each element represents
 * an individual row returned from the read operation. Both Query and Read APIs are supported.
 * See more information about <a href="https://cloud.google.com/spanner/docs/reads">reading from
 * Cloud Spanner</a>
 *
 * <p>To execute a <strong>query</strong>, specify a {@link SpannerIO.Read#withQuery(Statement)} or
 * {@link SpannerIO.Read#withQuery(String)} during the construction of the transform.
 *
 * <pre>{@code
 *  PCollection<Struct> rows = p.apply(
 *      SpannerIO.read()
 *          .withInstanceId(instanceId)
 *          .withDatabaseId(dbId)
 *          .withQuery("SELECT id, name, email FROM users"));
 * }</pre>
 *
 * <p>To use the Read API, specify a {@link SpannerIO.Read#withTable(String) table name} and
 * a {@link SpannerIO.Read#withColumns(List) list of columns}.
 *
 * <pre>{@code
 * PCollection<Struct> rows = p.apply(
 *    SpannerIO.read()
 *        .withInstanceId(instanceId)
 *        .withDatabaseId(dbId)
 *        .withTable("users")
 *        .withColumns("id", "name", "email")
 * }</pre>
 *
 * <p>To optimally read using index, specify the index name using {@link SpannerIO.Read#withIndex}.
 *
 * <p>The transform is guaranteed to be executed on a consistent snapshot of data, utilizing the
 * power of read only transactions. Staleness of data can be controlled using
 * {@link SpannerIO.Read#withTimestampBound} or {@link SpannerIO.Read#withTimestamp(Timestamp)}
 * methods. <a href="https://cloud.google.com/spanner/docs/transactions">Read more</a> about
 * transactions in Cloud Spanner.
 *
 * <p>It is possible to read several {@link PCollection PCollections} within a single transaction.
 * Apply {@link SpannerIO#createTransaction()} transform, that lazily creates a transaction. The
 * result of this transformation can be passed to read operation using
 * {@link SpannerIO.Read#withTransaction(PCollectionView)}.
 *
 * <pre>{@code
 * SpannerConfig spannerConfig = ...
 *
 * PCollectionView<Transaction> tx =
 * p.apply(
 *    SpannerIO.createTransaction()
 *        .withSpannerConfig(spannerConfig)
 *        .withTimestampBound(TimestampBound.strong()));
 *
 * PCollection<Struct> users = p.apply(
 *    SpannerIO.read()
 *        .withSpannerConfig(spannerConfig)
 *        .withQuery("SELECT name, email FROM users")
 *        .withTransaction(tx));
 *
 * PCollection<Struct> tweets = p.apply(
 *    SpannerIO.read()
 *        .withSpannerConfig(spannerConfig)
 *        .withQuery("SELECT user, tweet, date FROM tweets")
 *        .withTransaction(tx));
 * }</pre>
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
 *   <li>Mutations are not submitted atomically;
 *   <li>A mutation is applied at least once;
 *   <li>If the pipeline was unexpectedly stopped, mutations that were already applied will not get
 *       rolled back.
 * </ul>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024 * 1024; // 1 MB

  /**
   * Creates an uninitialized instance of {@link Read}. Before use, the {@link Read} must be
   * configured with a {@link Read#withInstanceId} and {@link Read#withDatabaseId} that identify the
   * Cloud Spanner database.
   */
  @Experimental
  public static Read read() {
    return new AutoValue_SpannerIO_Read.Builder()
        .setTimestampBound(TimestampBound.strong())
        .setKeySet(KeySet.all())
        .build();
  }

  /**
   * Returns a transform that creates a batch transaction. By default,
   * {@link TimestampBound#strong()} transaction is created, to override this use
   * {@link CreateTransaction#withTimestampBound(TimestampBound)}.
   */
  @Experimental
  public static CreateTransaction createTransaction() {
    return new AutoValue_SpannerIO_CreateTransaction.Builder()
        .setTimestampBound(TimestampBound.strong())
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
    abstract SpannerConfig getSpannerConfig();

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

    @Nullable
    abstract PCollectionView<Transaction> getTransaction();

    abstract Builder toBuilder();

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

    public Read withIndex(String index) {
      return toBuilder().setIndex(index).build();
    }

    public Read withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract SpannerConfig.Builder spannerConfigBuilder();

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setQuery(Statement statement);

      abstract Builder setTable(String table);

      abstract Builder setIndex(String index);

      abstract Builder setColumns(List<String> columns);

      abstract Builder setKeySet(KeySet keySet);

      abstract Builder setTransaction(PCollectionView<Transaction> transaction);

      abstract Read build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public Read withProjectId(String projectId) {
      Builder builder = toBuilder();
      builder.spannerConfigBuilder().setProjectId(projectId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Read withInstanceId(String instanceId) {
      Builder builder = toBuilder();
      builder.spannerConfigBuilder().setInstanceId(instanceId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * database.
     *
     * <p>Does not modify this object.
     */
    public Read withDatabaseId(String databaseId) {
      Builder builder = toBuilder();
      builder.spannerConfigBuilder().setDatabaseId(databaseId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * config.
     *
     * <p>Does not modify this object.
     */
    public Read withTransaction(PCollectionView<Transaction> transaction) {
      return toBuilder().setTransaction(transaction).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      getSpannerConfig().validate(options);
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
      Read config = this;
      List<PCollectionView<Transaction>> sideInputs = Collections.emptyList();
      if (getTimestampBound() != null) {
        PCollectionView<Transaction> transaction =
            input.apply(createTransaction().withSpannerConfig(getSpannerConfig()));
        config = config.withTransaction(transaction);
        sideInputs = Collections.singletonList(transaction);
      }
      return input
          .apply(Create.of(1))
          .apply(
              "Execute query", ParDo.of(new NaiveSpannerReadFn(config)).withSideInputs(sideInputs));
    }
  }

  /**
   * A {@link PTransform} that create a transaction.
   *
   * @see SpannerIO
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class CreateTransaction
      extends PTransform<PBegin, PCollectionView<Transaction>> {
    abstract SpannerConfig getSpannerConfig();

    @Nullable
    abstract TimestampBound getTimestampBound();

    abstract Builder toBuilder();

    @Override
    public PCollectionView<Transaction> expand(PBegin input) {
      return input
          .apply(Create.of(1))
          .apply("Create transaction", ParDo.of(new CreateTransactionFn(this)))
          .apply("As PCollectionView", View.<Transaction>asSingleton());
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public CreateTransaction withProjectId(String projectId) {
      Builder builder = toBuilder();
      builder.spannerConfigBuilder().setProjectId(projectId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public CreateTransaction withInstanceId(String instanceId) {
      Builder builder = toBuilder();
      builder.spannerConfigBuilder().setInstanceId(instanceId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Read} that will read from the specified Cloud Spanner
     * database.
     *
     * <p>Does not modify this object.
     */
    public CreateTransaction withDatabaseId(String databaseId) {
      Builder builder = toBuilder();
      builder.spannerConfigBuilder().setDatabaseId(databaseId);
      return builder.build();
    }

    public CreateTransaction withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    CreateTransaction withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      getSpannerConfig().validate(options);
    }

    /** A builder for {@link CreateTransaction}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract SpannerConfig.Builder spannerConfigBuilder();

      public abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      public abstract Builder setTimestampBound(TimestampBound newTimestampBound);

      public abstract CreateTransaction build();
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

    abstract SpannerConfig getSpannerConfig();

    abstract long getBatchSizeBytes();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract SpannerConfig.Builder spannerConfigBuilder();

      abstract Builder setBatchSizeBytes(long batchSizeBytes);

      abstract Write build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner project.
     *
     * <p>Does not modify this object.
     */
    public Write withProjectId(String projectId) {
      Write.Builder builder = toBuilder();
      builder.spannerConfigBuilder().setProjectId(projectId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * instance.
     *
     * <p>Does not modify this object.
     */
    public Write withInstanceId(String instanceId) {
      Write.Builder builder = toBuilder();
      builder.spannerConfigBuilder().setInstanceId(instanceId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * database.
     *
     * <p>Does not modify this object.
     */
    public Write withDatabaseId(String databaseId) {
      Write.Builder builder = toBuilder();
      builder.spannerConfigBuilder().setDatabaseId(databaseId);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} that will write to the specified Cloud Spanner
     * config.
     *
     * <p>Does not modify this object.
     */
    public Write withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    @VisibleForTesting
    Write withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      Write.Builder builder = toBuilder();
      builder.spannerConfigBuilder().setServiceFactory(serviceFactory);
      return builder.build();
    }

    /**
     * Returns a new {@link SpannerIO.Write} with a new batch size limit.
     *
     * <p>Does not modify this object.
     */
    public Write withBatchSizeBytes(long batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      getSpannerConfig().validate(options);
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input.apply("Write mutations to Cloud Spanner", ParDo.of(new SpannerWriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      getSpannerConfig().populateDisplayData(builder);
      builder.add(
          DisplayData.item("batchSizeBytes", getBatchSizeBytes()).withLabel("Batch Size in Bytes"));
    }
  }

  private SpannerIO() {} // Prevent construction.
}
