package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.*;

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

    PCollection<Map<String, List<KeyPart>>> pk = begin.apply(Create.of((Void) null))
        .apply("Read information schema", ParDo.of(new ReadPkInfo(config)));

    final PCollectionView<Map<String, List<KeyPart>>> pkView = pk
        .apply("Primary key view", View.<Map<String, List<KeyPart>>>asSingleton());

    PCollectionView<List<KV<String, Key>>> sample = input
        .apply("Calculate keys", ParDo.of(new ExtractKeyFn(pkView)).withSideInputs(pkView))
        .apply("Sample keys", Sample.<KV<String, Key>>fixedSizeGlobally(numSamples))
        .apply("Sort keys", ParDo.of(new SortSampleFn(pkView)).withSideInputs(pkView))
        .apply("Keys sample as view", View.<List<KV<String, Key>>>asSingleton());

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


  private static class ExtractKeyFn extends DoFn<MutationGroup, KV<String, Key>> {
    final PCollectionView<Map<String, List<KeyPart>>> pkView;

    private ExtractKeyFn(PCollectionView<Map<String, List<KeyPart>>> pkView) {
      this.pkView = pkView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation m = c.element().primary();
      Map<String, List<KeyPart>> pkMapping = c.sideInput(pkView);
      c.output(KV.of(m.getTable(), keyOf(m, pkMapping)));
    }
  }

  private static class SortSampleFn extends DoFn<Iterable<KV<String, Key>>, List<KV<String, Key>>> {
    final PCollectionView<Map<String, List<KeyPart>>> pkView;

    private SortSampleFn(PCollectionView<Map<String, List<KeyPart>>> pkView) {
      this.pkView = pkView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<KV<String, Key>> result = new ArrayList<>();
      Iterables.addAll(result, c.element());

      Map<String, List<KeyPart>> pkMapping = c.sideInput(pkView);
      Collections.sort(result, new TabledKeyComparator(pkMapping));
      c.output(result);
    }

  }

  private static class TabledKeyComparator implements Comparator<KV<String, Key>> {
    private final Map<String, List<KeyPart>> pkMapping;

    private TabledKeyComparator(Map<String, List<KeyPart>> pkMapping) {
      this.pkMapping = pkMapping;
    }

    @Override
    public int compare(KV<String, Key> a, KV<String, Key> b) {
      int result = a.getKey().compareTo(b.getKey());
      if (result != 0) {
        return result;
      }

      Iterator<Object> ai = a.getValue().getParts().iterator();
      Iterator<Object> bi = b.getValue().getParts().iterator();
      List<KeyPart> parts = pkMapping.get(a.getKey());
      for (KeyPart part : parts) {
        Object ao = ai.next();
        Object bo = bi.next();
        if (ao == null) {
          // Verify
          result = bo == null ? 0 : -1;
        } else if (bo == null) {
          result = 1;
        } else {
          result = ((Comparable) ao).compareTo(bo);
        }
        if (result != 0) {
          return result * (part.isDesc() ? - 1 : 1);
        }
      }
      return 0;
    }
  }

  private static class PartitionFn extends DoFn<MutationGroup, KV<Integer, MutationGroup>> {
    final PCollectionView<List<KV<String, Key>>> sampleView;
    final PCollectionView<Map<String, List<KeyPart>>> pkView;

    public PartitionFn(PCollectionView<List<KV<String, Key>>> sampleView,
        PCollectionView<Map<String, List<KeyPart>>> pkView) {
      this.sampleView = sampleView;
      this.pkView = pkView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<KV<String, Key>> sample = c.sideInput(sampleView);
      Map<String, List<KeyPart>> pkMapping = c.sideInput(pkView);

      MutationGroup g = c.element();
      KV<String, Key> key = KV.of(g.primary().getTable(), keyOf(g.primary(), pkMapping));
      int partition = Collections.binarySearch(sample, key, new TabledKeyComparator(pkMapping));
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
        if (batchSizeBytes >= maxBatchSizeBytes || mutations.size() > 1500) {
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
