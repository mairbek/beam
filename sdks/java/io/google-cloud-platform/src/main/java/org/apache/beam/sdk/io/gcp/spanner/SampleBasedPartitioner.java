package org.apache.beam.sdk.io.gcp.spanner;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SampleBasedPartitioner extends PTransform<PCollection<KV<String, MutationGroup>>,
        PCollection<KV<String, MutationGroup>>> {

    private final KeyComparator keyComparator;
    private final int numQuantiles;

    public SampleBasedPartitioner(KeyComparator keyComparator, int numQuantiles) {
        this.keyComparator = keyComparator;
        this.numQuantiles = numQuantiles;
    }

    @Override
    public PCollection<KV<String, MutationGroup>> expand(PCollection<KV<String, MutationGroup>> input) {
        PCollectionView<Map<String, List<MutationGroup>>> quantilesView = input.apply
                ("Calculate quantiles",
                        ApproximateQuantiles.<String, MutationGroup, KeyComparator>perKey(numQuantiles, keyComparator))
                .apply("Materialize quantiles", View.<String, List<MutationGroup>>asMap());
        return input.apply("Partition", ParDo.of(new PartitionFn(keyComparator, quantilesView))
                .withSideInputs(quantilesView));
    }

    private static class PartitionFn extends DoFn<KV<String, MutationGroup>, KV<String, MutationGroup>> {
        private final KeyComparator keyComparator;
        private final PCollectionView<Map<String, List<MutationGroup>>> view;

        private PartitionFn(KeyComparator keyComparator, PCollectionView<Map<String, List<MutationGroup>>> view) {
            this.keyComparator = keyComparator;
            this.view = view;

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, MutationGroup> kv = c.element();

            Map<String, List<MutationGroup>> quantiles = c.sideInput(view);

            String table = kv.getKey();
            MutationGroup mutationGroup = kv.getValue();
            int position = Collections.binarySearch(quantiles.get(table),
                    mutationGroup, keyComparator);
            String partition = String.valueOf(Math.abs(position));
            KV<String, MutationGroup> result = KV.of(table + "@" + partition, mutationGroup);
            c.output(result);
        }


    }
}
