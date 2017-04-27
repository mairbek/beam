package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class SpannerWriteFn extends DoFn<Iterable<MutationGroup>, MutationGroup> {
    private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteFn.class);
    private final String instanceId;
    private final String databaseId;
    private transient Spanner spanner;
    private transient DatabaseClient dbClient;

    @VisibleForTesting
    SpannerWriteFn(String instanceId, String databaseId) {
        this.instanceId = checkNotNull(instanceId, "instanceId");
        this.databaseId = checkNotNull(databaseId, "databaseId");
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
        Iterable<MutationGroup> batch = c.element();

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
    }

    @Teardown
    public void teardown() throws Exception {
        if (spanner == null) {
            return;
        }
        spanner.closeAsync().get();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder
                .addIfNotNull(DisplayData.item("instanceId", instanceId)
                        .withLabel("Instance"))
                .addIfNotNull(DisplayData.item("databaseId", databaseId)
                        .withLabel("Database"));
    }
}
