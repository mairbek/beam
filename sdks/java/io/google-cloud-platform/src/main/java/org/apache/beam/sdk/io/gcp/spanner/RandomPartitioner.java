package org.apache.beam.sdk.io.gcp.spanner;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Random;

public class RandomPartitioner extends PTransform<PCollection<KV<String, MutationGroup>>,
        PCollection<KV<String, MutationGroup>>> {
    private final Random random = new Random();
    private final int numPartitions;

    public RandomPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public PCollection<KV<String, MutationGroup>> expand(PCollection<KV<String, MutationGroup>> input) {
        return input.apply("Partition", ParDo.of(new DoFn<KV<String, MutationGroup>, KV<String, MutationGroup>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                KV<String, MutationGroup> kv = c.element();
                String table = kv.getKey();
                MutationGroup mutationGroup = kv.getValue();
                KV<String, MutationGroup> result = KV.of(table + "@" + random.nextInt(numPartitions),
                        mutationGroup);
                c.output(result);
            }
        }));

    }
}
