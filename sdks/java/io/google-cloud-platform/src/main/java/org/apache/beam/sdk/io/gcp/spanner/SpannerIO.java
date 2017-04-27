package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;


/**
 * <a href="https://cloud.google.com/spanner/">Google Cloud Spanner</a> connectors.
 * <p>
 * <h3>Reading from Cloud Spanner</h3>
 * <strong>Status: Not implemented.</strong>
 * <p>
 * <h3>Writing to Cloud Spanner</h3>
 * <strong>Status: Experimental.</strong>
 * <p>
 * {@link SpannerIO#writeTo} batches together and concurrently writes a set of {@link Mutation}s.
 * <p>
 * To configure Cloud Spanner sink, you must apply {@link SpannerIO#writeTo} transform to
 * {@link PCollection<Mutation>} and specify instance and database identifiers.
 * For example, following code sketches out a pipeline that imports data from the CSV file to Cloud
 * Spanner.
 * <p>
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

    private static final long DEFAULT_BATCH_SIZE = 1024 * 1024 * 8;  // 1 MB
    private static final int DEFAULT_NUM_PARTITIONS = 1000;

    private SpannerIO() {
    }

    /**
     * Creates an instance of {@link Writer}. Use {@link Writer#withBatchSize} to tweak the batch
     * size.
     */
    public static Writer writeTo(String instanceId, String databaseId) {
        return new Writer(instanceId, databaseId, DEFAULT_BATCH_SIZE);
    }

    /**
     * A {@link PTransform} that writes {@link Mutation} objects to Cloud Spanner.
     *
     * @see SpannerIO
     */
    public static class Writer extends PTransform<PCollection<Mutation>, PCollection<Mutation>> {
        private SpannerWriteFn writeFn;
        private FlattenFn flattenFn;
        private PTransform<PCollection<KV<String, MutationGroup>>, PCollection<KV<String,
                MutationGroup>>> partitioner;

        Writer(String instanceId, String databaseId, long batchSize) {
            writeFn = new SpannerWriteFn(instanceId, databaseId);
            flattenFn = new FlattenFn(batchSize);
            partitioner = new RandomPartitioner(DEFAULT_NUM_PARTITIONS);
        }

        /**
         * Returns a new {@link Writer} with a limit on the number of batch per batch.
         * Defaults to recommended 1MB.
         *
         * @param batchSize a max size of the batch in bits.
         */
        public Writer withBatchSize(long batchSize) {
            this.flattenFn = new FlattenFn(batchSize);
            return this;
        }

        public Writer sampleParitions(KeyComparator keyComparator, int numSamples) {
            partitioner = new SampleBasedPartitioner(keyComparator, numSamples);
            return this;
        }

        @Override
        public PCollection<Mutation> expand(PCollection<Mutation> input) {
            return input.apply(getWriterImpl());
        }

        private PTransform<PCollection<MutationGroup>,
                PCollection<MutationGroup>> getGroupedWriterImpl() {
            return new WriterImpl(writeFn, flattenFn, partitioner);
        }

        private PTransform<PCollection<Mutation>,
                PCollection<Mutation>> getWriterImpl() {
            return new MutationWrapperTransform(getGroupedWriterImpl());
        }


        @Override
        public String toString() {
            // TODO(mairbek): !!!
            return MoreObjects.toStringHelper(getClass())
                    .toString();
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            // TODO(mairbek): !!!
        }

        PTransform<PCollection<MutationGroup>, PCollection<MutationGroup>> grouped() {
            return new PTransform<PCollection<MutationGroup>, PCollection<MutationGroup>>() {
                @Override
                public PCollection<MutationGroup> expand(PCollection<MutationGroup> input) {
                    return input.apply(getGroupedWriterImpl());
                }
            };
        }
    }

    private static class MutationWrapperTransform extends PTransform<PCollection<Mutation>,
            PCollection<Mutation>> {
        private final PTransform<PCollection<MutationGroup>, PCollection<MutationGroup>> underlying;

        private MutationWrapperTransform(PTransform<PCollection<MutationGroup>, PCollection<MutationGroup>> underlying) {
            this.underlying = underlying;
        }

        @Override
        public PCollection<Mutation> expand(PCollection<Mutation> input) {
            PCollection<MutationGroup> grouped = input.apply("To grouped", ParDo.of(new DoFn<Mutation, MutationGroup>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    Mutation m = c.element();
                    c.output(MutationGroup.withPrimary(m).build());
                }
            }));
            PCollection<MutationGroup> result = grouped.apply(underlying);
            return result.apply(ParDo.of(new DoFn<MutationGroup, Mutation>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    MutationGroup g = c.element();
                    c.output(g.primary());
                }
            }));
        }
    }

    private static class WriterImpl extends PTransform<PCollection<MutationGroup>,
            PCollection<MutationGroup>> {

        private final SpannerWriteFn writeFn;
        private final FlattenFn flattenFn;
        private final PTransform<PCollection<KV<String, MutationGroup>>,
                PCollection<KV<String, MutationGroup>>> partitioner;

        public WriterImpl(SpannerWriteFn writeFn, FlattenFn flattenFn, PTransform<PCollection<KV<String, MutationGroup>>,
                PCollection<KV<String, MutationGroup>>> partitioner) {
            this.writeFn = writeFn;
            this.flattenFn = flattenFn;
            this.partitioner = partitioner;
        }

        @Override
        public PCollection<MutationGroup> expand(PCollection<MutationGroup> input) {
            PCollection<KV<String, MutationGroup>> tabled = input.apply("Extract table name", ParDo
                    .of(
                            new DoFn<MutationGroup, KV<String, MutationGroup>>() {
                                @ProcessElement
                                public void processElement(ProcessContext c) throws Exception {
                                    MutationGroup group = c.element();
                                    c.output(KV.of(group.primary().getTable(), group));
                                }
                            }));
            PCollection<KV<String, MutationGroup>> partitioned = tabled.apply("Apply partitioner", partitioner);
            PCollection<KV<String, Iterable<MutationGroup>>> grouped = partitioned.apply
                    ("Group by key", GroupByKey.<String, MutationGroup>create());

            PCollection<Iterable<MutationGroup>> batched = grouped.apply("Flatten", ParDo.of(flattenFn));
            return batched.apply("Write batch", ParDo.of(writeFn));
        }
    }

    private static class FlattenFn extends DoFn<KV<String, Iterable<MutationGroup>>,
            Iterable<MutationGroup>> {

        private final long maxBatchSize;

        private FlattenFn(long maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            List<MutationGroup> batch = new ArrayList<>();
            KV<String, Iterable<MutationGroup>> kv = c.element();
            long batchSize = 0;
            for (MutationGroup group : kv.getValue()) {
                batch.add(group);
                batchSize += MutationSizeEstimator.sizeOf(group);
                if (batchSize >= maxBatchSize) {
                    c.output(batch);
                }
            }
            if (!batch.isEmpty()) {
                c.output(batch);
            }
        }
    }

}
