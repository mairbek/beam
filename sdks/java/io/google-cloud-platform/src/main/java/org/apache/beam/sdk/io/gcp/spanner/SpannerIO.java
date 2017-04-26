package org.apache.beam.sdk.io.gcp.spanner;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.cloud.spanner.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 *  <a href="https://cloud.google.com/spanner/">Google Cloud Spanner</a> connectors.
 *
 * <h3>Reading from Cloud Spanner</h3>
 * <strong>Status: Not implemented.</strong>
 *
 * <h3>Writing to Cloud Spanner</h3>
 * <strong>Status: Experimental.</strong>
 * <p>
 * {@link SpannerIO#writeTo} batches together and concurrently writes a set of {@link Mutation}s.
 *
 * To configure Cloud Spanner sink, you must apply {@link SpannerIO#writeTo} transform to
 * {@link PCollection<Mutation>} and specify instance and database identifiers.
 * For example, following code sketches out a pipeline that imports data from the CSV file to Cloud
 * Spanner.
 *
 * <pre>{@code
 *
 * Pipeline p = ...;
 * // Read the CSV file.
 * PCollection<String> lines = p.apply("Read CSV file", TextIO.Read.from(options.getInput()));
 * // Parse the line and convert to mutation.
 * PCollection<Mutation> mutations = lines.apply("Parse CSV", parseFromCsv());
 * // Write mutations.
 * mutations.apply("Write", SpannerIO.writeTo(options.getInstanceId(), options.getDatabaseId()));
 * p.run();
 *
 * }</pre>

 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SpannerIO {

  private SpannerIO() {
  }

  private static final long MAX_BATCH_SIZE = 1024 * 1024 * 8;  // 1 MB

    /**
     * Creates an instance of {@link Writer}. Use {@link Writer#withBatchSize} to limit the batch
     * size.
     */
  public static Writer writeTo(String instanceId, String databaseId) {
    return new Writer(instanceId, databaseId, MAX_BATCH_SIZE);
  }

  /**
   * A {@link PTransform} that writes {@link Mutation} objects to Cloud Spanner.
   *
   * @see SpannerIO
   */
  public static class Writer extends PTransform<PCollection<Mutation>, PDone> {

    private final String instanceId;
    private final String databaseId;
    private long batchSize;

    Writer(String instanceId, String databaseId, long batchSize) {
      this.instanceId = instanceId;
      this.databaseId = databaseId;
      this.batchSize = batchSize;
    }

    /**
     * Returns a new {@link Writer} with a limit on the number of mutations per batch.
     * Defaults to recommended 1MB.
     *
     * @param batchSize a max size of the batch in bits.
     */
    public Writer withBatchSize(long batchSize) {
      return new Writer(instanceId, databaseId, batchSize);
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      input.apply("Convert to group", ParDo.of(new SpannerGroupFn()))
              .apply("Write mutations to Spanner", ParDo.of(
                      new SpannerWriterFn(instanceId, databaseId, batchSize)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<Mutation> input) {
      checkNotNull(instanceId, "instanceId");
      checkNotNull(databaseId, "databaseId");
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("instanceId", instanceId)
          .add("databaseId", databaseId)
          .toString();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
              .withLabel("Output Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
              .withLabel("Output Database"));
    }

    PTransform<PCollection<MutationGroup>, PDone> grouped() {
      return new PTransform<PCollection<MutationGroup>, PDone>() {
        @Override
        public PDone expand(PCollection<MutationGroup> input) {
          input.apply("Write mutations to Spanner", ParDo.of(
                  new SpannerWriterFn(instanceId, databaseId, batchSize)));
          return PDone.in(input.getPipeline());
        }
      };
    }
  }

  private static class SpannerGroupFn extends DoFn<Mutation, MutationGroup> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation m = c.element();
      c.output(MutationGroup.withPrimary(m).build());
    }
  }


    /**
   * {@link DoFn} that writes {@link Mutation}s to Cloud Spanner. Mutations are written in
   * batches.
   *
   * <p>See <a href="https://cloud.google.com/spanner"/>
   *
   * <p>Commits are non-transactional.  If a commit fails, it will be retried (up to
   * {@link #MAX_RETRIES}. times). This means that the
   * mutation operation should be idempotent.
   */
  @VisibleForTesting
  static class SpannerWriterFn extends DoFn<MutationGroup, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterFn.class);
    private transient Spanner spanner;
    private final String instanceId;
    private final String databaseId;
    private final long maxBatchSize;
    private transient DatabaseClient dbClient;
    // Current batch of mutations to be written.
    private final List<Mutation> mutations = new ArrayList<>();
    // The size of current batch.
    private long batchSize = 0;

    private static final int MAX_RETRIES = 5;
    private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));

    @VisibleForTesting
    SpannerWriterFn(String instanceId, String databaseId, long maxBatchSize) {
      this.instanceId = checkNotNull(instanceId, "instanceId");
      this.databaseId = checkNotNull(databaseId, "databaseId");
      this.maxBatchSize = maxBatchSize;
    }

    @Setup
    public void setup() throws Exception {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        spanner = options.getService();
        dbClient = spanner.getDatabaseClient(
            DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      MutationGroup m = c.element();
      mutations.addAll(m.allMutations());
      batchSize += MutationSizeEstimator.sizeOf(m);
      if (batchSize >= maxBatchSize) {
        flushBatch();
      }
    }

    @FinishBundle
    public void finishBundle(Context c) throws Exception {
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
      LOG.debug("Writing batch of {} mutations. Total size {} bits", mutations.size(), batchSize);
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
      batchSize = 0;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("instanceId", instanceId)
              .withLabel("Instance"))
          .addIfNotNull(DisplayData.item("databaseId", databaseId)
              .withLabel("Database"));
    }
  }
}
