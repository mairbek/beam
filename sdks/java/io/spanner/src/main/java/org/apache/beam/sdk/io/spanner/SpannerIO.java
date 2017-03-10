package org.apache.beam.sdk.io.spanner;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SpannerIO {

    // TODO(mairbek): Builder BS
    public static Write write(String projectId, String instanceId, String databaseId, String
            table) {
        return new Write(projectId, instanceId, databaseId, table);
    }

    public static class Write
            extends PTransform<PCollection<InsertOrUpdate>, PDone> {

        // TODO(mairbek): DatabaseId must be serializable.
        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final String table;

        Write(String projectId, String instanceId, String databaseId, String table) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.table = table;
        }

        @Override
        public PDone expand(PCollection<InsertOrUpdate> input) {
            PCollection<KV<String, InsertOrUpdate>> kvs = input.apply("Guess partitions", ParDo.of(new RandomPartition()));
            PCollection<KV<String, Iterable<InsertOrUpdate>>> grouped = kvs.apply("Group by partitions", GroupByKey.<String, InsertOrUpdate>create());
            PCollection<Void> result = grouped.apply("Write mutations", ParDo.of(new SpannerWriterFn(projectId, instanceId, databaseId, table)));
            return PDone.in(result.getPipeline());
        }
    }

    private static class RandomPartition extends DoFn<InsertOrUpdate, KV<String, InsertOrUpdate>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            InsertOrUpdate mutation = c.element();
            String key = UUID.randomUUID().toString();
            c.output(KV.of(key, mutation));
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
            for (InsertOrUpdate write : writes) {
                Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
                for (Map.Entry<String, Value> kv : write.asMap().entrySet()) {
                    // TODO(mairbek): allow setting value
                    builder.set(kv.getKey()).to(kv.getValue().getString());
                }
                mutations.add(builder.build());
            }
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
