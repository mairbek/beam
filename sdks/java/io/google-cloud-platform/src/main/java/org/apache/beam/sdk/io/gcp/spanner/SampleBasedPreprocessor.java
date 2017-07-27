package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SampleBasedPreprocessor extends PTransform<PCollection<MutationGroup>,
    PCollection<Iterable<MutationGroup>>> {

  private final SpannerConfig config;
  private final int numSamples;
  private final long maxBatchSizeBytes;

  public SampleBasedPreprocessor(SpannerConfig config, int numSamples,
      long maxBatchSizeBytes) {
    this.config = config;
    this.numSamples = numSamples;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
  }

  @Override
  public PCollection<Iterable<MutationGroup>> expand(PCollection<MutationGroup> input) {
    PBegin begin = input.getPipeline().begin();

    Combine.Globally<SampleKey, List<SampleKey>> approximateQuantiles = Combine.globally(
        ApproximateQuantiles.ApproximateQuantilesCombineFn
            .create(numSamples, new Top.Natural<SampleKey>(), (long) 1e5, 1. / numSamples));

    PCollection<Map<String, List<KeyPart>>> pk = begin.apply(Create.of((Void) null))
        .apply("Read information schema", ParDo.of(new ReadPkInfo(config)));

    final PCollectionView<Map<String, List<KeyPart>>> pkView = pk
        .apply("Primary key view", View.<Map<String, List<KeyPart>>>asSingleton());

    PCollectionView<List<SampleKey>> sample = input
        .apply("Calculate keys", ParDo.of(new ExtractKeyFn(pkView)).withSideInputs(pkView))
        .apply("Sample keys", approximateQuantiles)
        .apply("Keys sample as view", View.<List<SampleKey>>asSingleton());

    return input
        .apply("Partition input",
            ParDo.of(new PartitionFn(sample, pkView)).withSideInputs(sample, pkView))
        .apply("Group by partition", GroupByKey.<Integer, MutationGroup>create())
        .apply("Presort and batch", ParDo.of(new PresortAndBatchFn(maxBatchSizeBytes)));
  }

  private static Key keyOf(Mutation m, Map<String, List<KeyPart>> mapping) {
    List<KeyPart> parts = mapping.get(m.getTable());
    Key.Builder keyBuilder = Key.newBuilder();
    Map<String, Value> mutationMap = m.asMap();
    for (KeyPart keyPart : parts) {
      Object result = null;
      Value value = mutationMap.get(keyPart.getField());
      if (!value.isNull()) {
        Type.Code code = value.getType().getCode();
        switch (code) {
          case BOOL:
            result = value.getBool();
            break;
          case INT64:
            result = value.getInt64();
            break;
          case FLOAT64:
            result = value.getFloat64();
            break;
          case STRING:
            result = value.getString();
            break;
          case BYTES:
            result = value.getBytes();
            break;
          case TIMESTAMP:
            result = value.getTimestamp();
            break;
          case DATE:
            result = value.getDate();
            break;
          default:
            throw new IllegalArgumentException("!!!");
        }
      }
      keyBuilder.appendObject(result);
    }
    return keyBuilder.build();
  }


  private static class ExtractKeyFn extends DoFn<MutationGroup, SampleKey> {
    final PCollectionView<Map<String, List<KeyPart>>> pkView;

    private ExtractKeyFn(PCollectionView<Map<String, List<KeyPart>>> pkView) {
      this.pkView = pkView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation m = c.element().primary();
      Map<String, List<KeyPart>> pkMapping = c.sideInput(pkView);
      Key key = keyOf(m, pkMapping);
      String table = m.getTable();
      c.output(SampleKey.create(table, key, pkMapping.get(table)));
    }
  }

  private static class PartitionFn extends DoFn<MutationGroup, KV<Integer, MutationGroup>> {
    final PCollectionView<List<SampleKey>> sampleView;
    final PCollectionView<Map<String, List<KeyPart>>> pkView;

    public PartitionFn(PCollectionView<List<SampleKey>> sampleView,
        PCollectionView<Map<String, List<KeyPart>>> pkView) {
      this.sampleView = sampleView;
      this.pkView = pkView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<SampleKey> sample = c.sideInput(sampleView);
      Map<String, List<KeyPart>> pkMapping = c.sideInput(pkView);

      MutationGroup g = c.element();
      Key key = keyOf(g.primary(), pkMapping);
      String table = g.primary().getTable();
      SampleKey sampleKey = SampleKey.create(table, key, pkMapping.get(table));
      int partition = Collections.binarySearch(sample, sampleKey);
      c.output(KV.of(partition, g));
    }

  }

  private static class PresortAndBatchFn
      extends DoFn<KV<Integer, Iterable<MutationGroup>>, Iterable<MutationGroup>> {

    private final long maxBatchSizeBytes;
    // Current batch of mutations to be written.
    private List<MutationGroup> mutations;
    private long batchSizeBytes = 0;

    private PresortAndBatchFn(long maxBatchSizeBytes) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
    }

    @Setup
    public void setup() throws Exception {
      mutations = new ArrayList<>();
      batchSizeBytes = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, Iterable<MutationGroup>> element = c.element();
      for (MutationGroup mg : element.getValue()) {
        mutations.add(mg);
        batchSizeBytes += MutationSizeEstimator.sizeOf(mg);
        if (batchSizeBytes >= maxBatchSizeBytes || mutations.size() > 1000) {
          c.output(mutations);
          mutations = new ArrayList<>();
          batchSizeBytes = 0;
        }
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      if (!mutations.isEmpty()) {
        c.output(mutations, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
        batchSizeBytes = 0;
      }
    }

  }
}
