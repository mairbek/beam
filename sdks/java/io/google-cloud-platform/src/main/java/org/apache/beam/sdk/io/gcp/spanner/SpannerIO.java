package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.*;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.UUID;

public class SpannerIO {

    // TODO(mairbek): Builder BS
    public static Write write(String projectId, String instanceId, String databaseId) {
        return new Write(projectId, instanceId, databaseId);
    }

    public static class Write
            extends PTransform<PCollection<Mutation>, PDone> {

        // TODO(mairbek): DatabaseId must be serializable.
        private final String projectId;
        private final String instanceId;
        private final String databaseId;

        public Write(String projectId, String instanceId, String databaseId) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
        }

        @Override
        public PDone expand(PCollection<Mutation> input) {
            PCollection<KV<String, Mutation>> kvs = input.apply("Guess partitions", ParDo.of(new RandomPartition()));
            PCollection<KV<String, Iterable<Mutation>>> grouped = kvs.apply("Group by partitions", GroupByKey.<String, Mutation>create());
            PCollection<Void> result = grouped.apply("Write mutations", ParDo.of(new SpannerWriterFn(projectId, instanceId, databaseId)));
            return PDone.in(result.getPipeline());
        }
    }

    private static class RandomPartition extends DoFn<Mutation, KV<String, Mutation>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Mutation mutation = c.element();
            String key = UUID.randomUUID().toString();
            c.output(KV.of(key, mutation));
        }
    }

    private static class SpannerWriterFn extends DoFn<KV<String, Iterable<Mutation>>, Void> {

        private static final Object lock = new Object();

        private final String projectId;
        private final String instanceId;
        private final String databaseId;

        private SpannerOptions spannerOptions;

        private DatabaseClient dbClient;
        private Spanner service;

        SpannerWriterFn(String projectId, String instanceId, String databaseId) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
        }

        @Setup
        public void setup() throws Exception {
            synchronized (lock) {
                System.out.println("!!!!");
                spannerOptions = SpannerOptions.newBuilder().setProjectId(projectId).build();
                DatabaseId db = DatabaseId.of(spannerOptions.getProjectId(), instanceId, databaseId);
                this.service = spannerOptions.getService();
                dbClient = service.getDatabaseClient(db);
                System.out.println("1111");
            }
        }

        @StartBundle
        public void startBundle(Context c) throws Exception {

        }

        @ProcessElement
        public void processElement(ProcessContext ctx) throws Exception {
            KV<String, Iterable<Mutation>> record = ctx.element();
            dbClient.writeAtLeastOnce(record.getValue());
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
