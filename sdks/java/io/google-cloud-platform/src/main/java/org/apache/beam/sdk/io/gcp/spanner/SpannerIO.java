package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

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
 * PCollection<Mutation> batch = lines.apply("Parse CSV", parseFromCsv());
 * // Write batch.
 * batch.apply("Write", SpannerIO.writeTo(options.getInstanceId(), options.getDatabaseId()));
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
   * Creates an instance of {@link Writer}. Use {@link Writer#withBatchSize} to tweak the batch
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
  public static class Writer extends PTransform<PCollection<Mutation>, PCollection<Mutation>> {

    private final String instanceId;
    private final String databaseId;
    private long batchSize;

    Writer(String instanceId, String databaseId, long batchSize) {
      this.instanceId = instanceId;
      this.databaseId = databaseId;
      this.batchSize = batchSize;
    }

    /**
     * Returns a new {@link Writer} with a limit on the number of batch per batch.
     * Defaults to recommended 1MB.
     *
     * @param batchSize a max size of the batch in bits.
     */
    public Writer withBatchSize(long batchSize) {
      return new Writer(instanceId, databaseId, batchSize);
    }

    @Override
    public PCollection<Mutation> expand(PCollection<Mutation> input) {
      // TODO(marbek): generic transformation code!
      return input.apply("Convert to group", ParDo.of(new ConvertToGroupFn()))
              .apply("Write batch to Spanner", ParDo.of(
                      new SpannerWriterFn(instanceId, databaseId, batchSize)))
              .apply("Convert failed mutations back", ParDo.of(new ConvertToMutationFn()));
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
          input.apply("Write batch to Spanner", ParDo.of(
                  new SpannerWriterFn(instanceId, databaseId, batchSize)));
          return PDone.in(input.getPipeline());
        }
      };
    }
  }

  private static class ConvertToGroupFn extends DoFn<Mutation, MutationGroup> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Mutation m = c.element();
      c.output(MutationGroup.withPrimary(m).build());
    }
  }

  private static class ConvertToMutationFn extends DoFn<MutationGroup, Mutation> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      MutationGroup g = c.element();
      c.output(g.primary());
    }
  }

  static class PartitionFn extends PTransform<PCollection<MutationGroup>,
          PCollection<Iterable<MutationGroup>>> {

    private long maxBatchSize = 10 * 1024 * 1024;

    @Override
    public PCollection<Iterable<MutationGroup>> expand(PCollection<MutationGroup> input) {
      PCollection<KV<String, MutationGroup>> tabled = input.apply("Extract table name", ParDo
              .of(
                      new DoFn<MutationGroup, KV<String, MutationGroup>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) throws Exception {
                          MutationGroup group = c.element();
                          c.output(KV.of(group.primary().getTable(), group));
                        }
                      }));

      final KeyComparator key = KeyComparator.builder().asc("key").build();

      final PCollectionView<Map<String, List<MutationGroup>>> quantilesView = tabled.apply
              ("Calculate " +
                              "Quantiles",
                      ApproximateQuantiles.<String, MutationGroup, KeyComparator>perKey(1000, key))
              .apply("Materialize quantiles", View.<String, List<MutationGroup>>asMap());

      PCollection<KV<String, MutationGroup>> partitioned = tabled.apply("Partition", ParDo.of(new
                                                                                                      DoFn<KV<String, MutationGroup>, KV<String, MutationGroup>>() {
                                                                                                        @ProcessElement
                                                                                                        public void processElement(ProcessContext c) {
                                                                                                          KV<String, MutationGroup> kv = c.element();

                                                                                                          Map<String, List<MutationGroup>> quantiles = c.sideInput
                                                                                                                  (quantilesView);

                                                                                                          String table = kv.getKey();
                                                                                                          MutationGroup mutationGroup = kv.getValue();
                                                                                                          int position = Collections.binarySearch(quantiles.get(table),
                                                                                                                  mutationGroup, key);
                                                                                                          String partition = String.valueOf(Math.abs(position));
                                                                                                          c.output(KV.of(table + "@" + partition, mutationGroup));
                                                                                                        }
                                                                                                      }

      ).withSideInputs(quantilesView));

      PCollection<KV<String, Iterable<MutationGroup>>> grouped = partitioned.apply
              ("Group by key", GroupByKey.<String, MutationGroup>create());

      grouped.apply("Flatten", ParDo.of(new
                                                DoFn<KV<String, Iterable<MutationGroup>>, ImmutableList<MutationGroup>>() {
                                                  @ProcessElement
                                                  public void processElement(ProcessContext c) {
                                                    ImmutableList.Builder<MutationGroup> batch = ImmutableList.builder();
                                                    KV<String, Iterable<MutationGroup>> kv = c.element();
                                                    long batchSize = 0;
                                                    for (MutationGroup group : kv.getValue()) {
                                                      batch.add(group);
                                                      batchSize += MutationSizeEstimator.sizeOf(group);
                                                      if (batchSize >= maxBatchSize) {
                                                        c.output(batch.build());
                                                        batch = ImmutableList.builder();
                                                      }
                                                    }
                                                    ImmutableList<MutationGroup> build = batch.build();
                                                    if (!build.isEmpty()) {
                                                      c.output(build);
                                                    }
                                                  }
                                                }));

      return null;
    }
  }

  public static class KeyComparator implements Comparator<MutationGroup>, Serializable {
    private final ImmutableList<String> fields;
    private final ImmutableList<Ordering> orderings;

    private KeyComparator(ImmutableList<String> fields, ImmutableList<Ordering> orderings) {
      this.fields = fields;
      this.orderings = orderings;
    }

    public static Builder builder() {
      return new Builder();
    }

    private static int compareValues(Value v1, Value v2) {
      if (v1.isNull()) {
        return v2.isNull() ? 0 : 1;
      }
      if (!v1.getType().equals(v2.getType())) {
        throw new IllegalArgumentException("value type is different");
      }
      switch (v1.getType().getCode()) {
        case BOOL:
          return Boolean.compare(v1.getBool(), v2.getBool());
        case INT64:
          return Long.compare(v1.getInt64(), v2.getInt64());
        case FLOAT64:
          return Double.compare(v1.getFloat64(), v2.getFloat64());
        case STRING:
          return v1.getString().compareTo(v2.getString());
        // TODO(mairbek): support other?
      }
      throw new IllegalArgumentException("Unsupported primary key type " + v1.getType());
    }

    @Override
    public int compare(MutationGroup a, MutationGroup b) {
      for (int i = 0; i < fields.size(); i++) {
        String field = fields.get(i);
        Ordering ordering = orderings.get(i);
        // TODO(mairbek): deletes!
        int compare = compareValues(a.primary().asMap().get(field), b.primary().asMap().get(field));
        if (compare != 0) {
          return ordering.getMultiplier() * compare;
        }
      }
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyComparator that = (KeyComparator) o;

      if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;
      return orderings != null ? orderings.equals(that.orderings) : that.orderings == null;
    }

    @Override
    public int hashCode() {
      int result = fields != null ? fields.hashCode() : 0;
      result = 31 * result + (orderings != null ? orderings.hashCode() : 0);
      return result;
    }

    private enum Ordering implements Serializable {
      ASC {
        @Override
        int getMultiplier() {
          return 1;
        }
      }, DESC {
        @Override
        int getMultiplier() {
          return -1;
        }
      };

      abstract int getMultiplier();
    }

    public static class Builder {
      private final ImmutableList.Builder<String> fields = ImmutableList.builder();
      private final ImmutableList.Builder<Ordering> orderings = ImmutableList.builder();

      private Builder() {
      }

      public Builder asc(String field) {
        fields.add(field);
        orderings.add(Ordering.ASC);
        return this;
      }

      public Builder desc(String field) {
        fields.add(field);
        orderings.add(Ordering.DESC);
        return this;
      }

      public KeyComparator build() {
        return new KeyComparator(fields.build(), orderings.build());
      }

    }
  }


    /**
   * {@link DoFn} that writes {@link Mutation}s to Cloud Spanner. Mutations are written in
   * batches.
   *
   * <p>See <a href="https://cloud.google.com/spanner"/>
   *
   * <p>Commits are non-transactional.
   */
  @VisibleForTesting
  static class SpannerWriterFn extends DoFn<MutationGroup, MutationGroup> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriterFn.class);
    private transient Spanner spanner;
    private final String instanceId;
    private final String databaseId;
    private final long maxBatchSize;
    private transient DatabaseClient dbClient;
    // Current batch of batch to be written.
    private final List<MutationGroup> batch = new ArrayList<>();
    // The size of current batch.
    private long batchSize = 0;

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
      MutationGroup group = c.element();
      batch.add(group);
      batchSize += MutationSizeEstimator.sizeOf(group);
      if (batchSize >= maxBatchSize) {
        flushBatch(c);
      }
    }

    @FinishBundle
    public void finishBundle(Context c) throws Exception {
      if (!batch.isEmpty()) {
        flushBatch(c);
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
     * Writes a batch of batch to Cloud Spanner.
     * TODO(mairbek): !!!
     */
    private void flushBatch(Context c) throws AbortedException, IOException, InterruptedException {
      LOG.debug("Writing batch of {} batch groups. Total size {} bits", batch.size(),
              batchSize);
      // Try submit as a bundle.
      Iterable<Mutation> allMutations = Iterables.concat(batch);
      boolean failed = false;
      try {
        dbClient.writeAtLeastOnce(allMutations);
      } catch (SpannerException e) {
        LOG.error("Error writing batch to Spanner ({}): {}", e.getCode(),
                e.getMessage());
        failed = true;
      }
      if (failed) {
        for (MutationGroup group : batch) {
          try {
            dbClient.writeAtLeastOnce(group);
          } catch (SpannerException e) {
            LOG.error("Error writing to Spanner ({}): {}", e.getCode(),
                    e.getMessage());
            c.output(group);
          }
        }
      }
      batch.clear();
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
