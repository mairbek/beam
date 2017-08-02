package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.common.primitives.UnsignedBytes;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.util.*;

public class SampleBasedPreprocessor extends PTransform<PCollection<MutationGroup>,
    PCollection<Iterable<MutationGroup>>> {

  public static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
  private final SpannerConfig config;
  private final int numSamples;
  private final long maxBatchSizeBytes;

  public SampleBasedPreprocessor(SpannerConfig config, int numSamples,
      long maxBatchSizeBytes) {
    this.config = config;
    this.numSamples = numSamples;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
  }

  private enum MyComparator implements Comparator<byte[]>, Serializable {
    INSTANCE {
      @Override public int compare(byte[] o1, byte[] o2) {
        return COMPARATOR.compare(o1, o2);
      }
    }
  }

  @Override
  public PCollection<Iterable<MutationGroup>> expand(PCollection<MutationGroup> input) {
    PBegin begin = input.getPipeline().begin();

    Combine.Globally<byte[], List<byte[]>> approximateQuantiles = Combine.globally(
        ApproximateQuantiles.ApproximateQuantilesCombineFn
            .create(numSamples, MyComparator.INSTANCE, (long) 1e6, 1. / numSamples));

    PCollection<Map<String, List<KeyPart>>> pk = begin.apply(Create.of((Void) null))
        .apply("Read information schema", ParDo.of(new ReadPkInfo(config)));

    final PCollectionView<Map<String, List<KeyPart>>> pkView = pk
        .apply("Primary key view", View.<Map<String, List<KeyPart>>>asSingleton());

    PCollection<List<byte[]>> values = input
        .apply("Calculate keys", ParDo.of(new ExtractKeyFn(pkView)).withSideInputs(pkView))
        .apply("Sample keys", approximateQuantiles);
    PCollectionView<List<byte[]>> sample = values
        .apply("Keys sample as view", View.<List<byte[]>>asSingleton());

    values.apply(ParDo.of(new DoFn<List<byte[]>, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        List<byte[]> element = c.element();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (byte[] value : element) {
          if (first) {
            first = false;
          } else {
            sb.append("\n");
          }
          OrderedCode code = new OrderedCode(value);

          String table = new String(code.readBytes());
          long val = code.readSignedNumIncreasing();
          sb.append(table).append(",").append(val);
        }
        c.output(sb.toString());
      }

    })).apply(TextIO.write().to("gs://mairbek/one"));


    PCollection<Iterable<MutationGroup>> result = input.apply("Partition input",
        ParDo.of(new PartitionFn(sample, pkView)).withSideInputs(sample, pkView))
        .apply("Group by partition", GroupByKey.<Integer, MutationGroup>create())
        .apply("Presort and batch", ParDo.of(new PresortAndBatchFn(maxBatchSizeBytes)));
    return result;
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


  private static class ExtractKeyFn extends DoFn<MutationGroup, byte[]> {
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
      c.output(KeyCoder.encode(table, key, pkMapping.get(table)));
    }
  }

  private static class PartitionFn extends DoFn<MutationGroup, KV<Integer, MutationGroup>> {
    final PCollectionView<List<byte[]>> sampleView;
    final PCollectionView<Map<String, List<KeyPart>>> pkView;

    public PartitionFn(PCollectionView<List<byte[]>> sampleView,
        PCollectionView<Map<String, List<KeyPart>>> pkView) {
      this.sampleView = sampleView;
      this.pkView = pkView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<byte[]> sample = c.sideInput(sampleView);
      Map<String, List<KeyPart>> pkMapping = c.sideInput(pkView);

      MutationGroup g = c.element();
      Key key = keyOf(g.primary(), pkMapping);
      String table = g.primary().getTable();
      byte[] sampleKey = KeyCoder.encode(table, key, pkMapping.get(table));
      int partition = Math.abs(Collections.binarySearch(sample, sampleKey, MyComparator.INSTANCE));
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
      if (!mutations.isEmpty()) {
        c.output(mutations);
        batchSizeBytes = 0;
      }
    }
    
  }
}
