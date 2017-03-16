package org.apache.beam.sdk.io.spanner;

import com.google.cloud.spanner.*;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

import java.util.*;

public class SpannerIO {

    // TODO(mairbek): Builder!
    public static Write write(String projectId, String instanceId, String databaseId, String
            table) {
        return new Write(projectId, instanceId, databaseId, table);
    }

    private static class RandomPartition extends DoFn<InsertOrUpdate, KV<String, InsertOrUpdate>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            InsertOrUpdate mutation = c.element();
            String key = UUID.randomUUID().toString();
            c.output(KV.of(key, mutation));
        }
    }

    private static class SamplePartition extends DoFn<InsertOrUpdate, KV<String, InsertOrUpdate>> {
        private final PCollectionView<List<String>> sampleView;

        private SamplePartition(PCollectionView<List<String>> sampleView) {
            this.sampleView = sampleView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            InsertOrUpdate mutation = c.element();
            List<String> partitions = c.sideInput(sampleView);
            int position = Collections.binarySearch(partitions, mutation.asMap().get("key").getString());
            String partition = String.valueOf(Math.abs(position));
            c.output(KV.of(partition, mutation));
        }

    }

    public static class Write
            extends PTransform<PCollection<InsertOrUpdate>, PDone> {

        // TODO(mairbek): DatabaseId must be serializable.
        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final String table;
        private int sampleSize;

        Write(String projectId, String instanceId, String databaseId, String table) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.table = table;
            sampleSize = 100; // TODO(mairbek): configurable
        }

        @Override
        public PDone expand(PCollection<InsertOrUpdate> input) {
            PCollection<Iterable<InsertOrUpdate>> sample = input.apply(Sample
                    .<InsertOrUpdate>fixedSizeGlobally(sampleSize));

            PCollection<List<String>> keys = sample.apply("Convert and sort", ParDo.of(new
                                                                                               DoFn<Iterable<InsertOrUpdate>,
                                                                                                       List<String>>() {
                                                                                                   @ProcessElement
                                                                                                   public void processElement(ProcessContext c) {
                                                                                                       Iterable<InsertOrUpdate> vals = c.element();
                                                                                                       List<String> list = new ArrayList<String>();
                                                                                                       for (InsertOrUpdate write : vals) {
                                                                                                           list.add(write.asMap().get("key").toString());
                                                                                                       }
                                                                                                       Collections.sort(list);
                                                                                                       c.output(list);
                                                                                                   }

                                                                                               }));

            PCollectionView<List<String>> sampleView = keys.apply(View.<List<String>>asSingleton());

            PCollection<KV<String, InsertOrUpdate>> kvs = input.apply("SpannerIO: Guess " +
                    "partitions", ParDo
                    .of(new SamplePartition(sampleView)).withSideInputs(sampleView));
            PCollection<KV<String, Iterable<InsertOrUpdate>>> grouped = kvs.apply("SpannerIO: Group by partitions", GroupByKey.<String, InsertOrUpdate>create());
            PCollection<Void> result = grouped.apply("SpannerIO: Write mutations", ParDo.of(new SpannerWriterFn(projectId, instanceId, databaseId, table)));
            return PDone.in(result.getPipeline());
        }
    }

    private static class SpannerWriterFn extends DoFn<KV<String, Iterable<InsertOrUpdate>>, Void> {

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final String table;

        private SpannerOptions spannerOptions;

        private DatabaseClient dbClient;
        private Spanner service;

        SpannerWriterFn(String projectId, String instanceId, String databaseId, String table) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.table = table;
        }

        @Setup
        public void setup() throws Exception {
            spannerOptions = SpannerOptions.newBuilder().setProjectId(projectId).build();
            DatabaseId db = DatabaseId.of(spannerOptions.getProjectId(), instanceId, databaseId);
            this.service = spannerOptions.getService();
            dbClient = service.getDatabaseClient(db);
        }

        @StartBundle
        public void startBundle(Context c) throws Exception {

        }

        @ProcessElement
        public void processElement(ProcessContext ctx) throws Exception {
            KV<String, Iterable<InsertOrUpdate>> record = ctx.element();
            Iterable<InsertOrUpdate> writes = record.getValue();
            List<Mutation> mutations = new ArrayList<>();
            StringBuilder keys = new StringBuilder();
            for (InsertOrUpdate write : writes) {
                keys.append(" ").append(write.asMap().get("key").toString());
                Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
                for (Map.Entry<String, Value> kv : write.asMap().entrySet()) {
                    // TODO(mairbek): allow setting value
                    builder.set(kv.getKey()).to(kv.getValue().getString());
                }
                mutations.add(builder.build());
            }
            System.out.println(record.getKey() + " " + keys.toString());
            dbClient.writeAtLeastOnce(mutations);
        }

        @FinishBundle
        public void finishBundle(Context c) throws Exception {

        }

        @Teardown
        public void tearDown() throws Exception {
            ListenableFuture<Void> future = service.closeAsync();
            future.get();
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {

        }
    }

}
