package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link MutationGroupEncoder}.
 */
public class MutationGroupEncoderTest {
  private SpannerSchema allTypesSchema;

  @Before
  public void setUp() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();

    builder.addColumn("test", "intkey", "INT64");
    builder.addKeyPart("test", "intkey", false);

    builder.addColumn("test", "bool", "BOOL");
    builder.addColumn("test", "int64", "INT64");
    builder.addColumn("test", "float64", "FLOAT64");
    builder.addColumn("test", "string", "STRING");
    builder.addColumn("test", "bytes", "BYTES");
    builder.addColumn("test", "timestamp", "TIMESTAMP");
    builder.addColumn("test", "date", "DATE");

    builder.addColumn("test", "nullbool", "BOOL");
    builder.addColumn("test", "nullint64", "INT64");
    builder.addColumn("test", "nullfloat64", "FLOAT64");
    builder.addColumn("test", "nullstring", "STRING");
    builder.addColumn("test", "nullbytes", "BYTES");
    builder.addColumn("test", "nulltimestamp", "TIMESTAMP");
    builder.addColumn("test", "nulldate", "DATE");

    builder.addColumn("test", "arrbool", "ARRAY<BOOL>");
    builder.addColumn("test", "arrint64", "ARRAY<INT64>");
    builder.addColumn("test", "arrfloat64", "ARRAY<FLOAT64>");
    builder.addColumn("test", "arrstring", "ARRAY<STRING>");
    builder.addColumn("test", "arrbytes", "ARRAY<BYTES>");
    builder.addColumn("test", "arrtimestamp", "ARRAY<TIMESTAMP>");
    builder.addColumn("test", "arrdate", "ARRAY<DATE>");

    builder.addColumn("test", "nullarrbool", "ARRAY<BOOL>");
    builder.addColumn("test", "nullarrint64", "ARRAY<INT64>");
    builder.addColumn("test", "nullarrfloat64", "ARRAY<FLOAT64>");
    builder.addColumn("test", "nullarrstring", "ARRAY<STRING>");
    builder.addColumn("test", "nullarrbytes", "ARRAY<BYTES>");
    builder.addColumn("test", "nullarrtimestamp", "ARRAY<TIMESTAMP>");
    builder.addColumn("test", "nullarrdate", "ARRAY<DATE>");

    allTypesSchema = builder.build();
  }

  @Test
  public void testAllTypes() throws Exception {
    serializeAndVerify(g(appendAllTypes(Mutation.newInsertOrUpdateBuilder("test")).build()));
    serializeAndVerify(g(appendAllTypes(Mutation.newInsertBuilder("test")).build()));
    serializeAndVerify(g(appendAllTypes(Mutation.newUpdateBuilder("test")).build()));
    serializeAndVerify(g(appendAllTypes(Mutation.newReplaceBuilder("test")).build()));
  }

  @Test
  public void testMutationCaseInsensitive() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();
    builder.addKeyPart("test", "bool_field", false);
    builder.addColumn("test", "bool_field", "BOOL");
    SpannerSchema schema = builder.build();

    Mutation mutation = Mutation.newInsertBuilder("TEsT").set("BoOL_FiELd").to(true).build();
    serializeAndVerify(g(mutation), schema);
  }

  @Test
  public void testDeleteCaseInsensitive() throws Exception {
    SpannerSchema.Builder builder = SpannerSchema.builder();
    builder.addKeyPart("test", "bool_field", false);
    builder.addColumn("test", "int_field", "INT64");
    SpannerSchema schema = builder.build();

    Mutation mutation = Mutation.delete("TeSt", Key.of(1L));
    serializeAndVerify(g(mutation), schema);
  }

  @Test
  public void testDeletes() throws Exception {
    serializeAndVerify(g(Mutation.delete("test", Key.of(1L))));
    serializeAndVerify(g(Mutation.delete("test", Key.of((Long) null))));

    KeySet allTypes = KeySet.newBuilder()
        .addKey(Key.of(1L))
        .addKey(Key.of((Long) null))
        .addKey(Key.of(1.2))
        .addKey(Key.of((Double) null))
        .addKey(Key.of("one"))
        .addKey(Key.of((String) null))
        .addKey(Key.of(ByteArray.fromBase64("abcd")))
        .addKey(Key.of((ByteArray) null))
        .addKey(Key.of(Timestamp.now()))
        .addKey(Key.of((Timestamp) null))
        .addKey(Key.of(Date.fromYearMonthDay(2012, 1, 1)))
        .addKey(Key.of((Date) null))
        .build();

    serializeAndVerify(g(Mutation.delete("test", allTypes)));

    serializeAndVerify(
        g(Mutation
            .delete("test", KeySet.range(KeyRange.closedClosed(Key.of(1L), Key.of(2L))))));
  }

  private Mutation.WriteBuilder appendAllTypes(Mutation.WriteBuilder builder) {
    Timestamp ts = Timestamp.now();
    Date date = Date.fromYearMonthDay(2017, 1, 1);
    return builder
        .set("bool").to(true)
        .set("int64").to(1L)
        .set("float64").to(1.0)
        .set("string").to("my string")
        .set("bytes").to(ByteArray.fromBase64("abcdedf"))
        .set("timestamp").to(ts)
        .set("date").to(date)

        .set("arrbool").toBoolArray(Arrays.asList(true, false, null, true, null, false))
        .set("arrint64").toInt64Array(Arrays.asList(10L, -12L, null, null, 100000L))
        .set("arrfloat64").toFloat64Array(Arrays.asList(10., -12.23, null, null, 100000.33231))
        .set("arrstring").toStringArray(Arrays.asList("one", "two", null, null, "three"))
        .set("arrbytes").toBytesArray(Arrays.asList(ByteArray.fromBase64("abcs"), null))
        .set("arrtimestamp").toTimestampArray(Arrays.asList(Timestamp.MIN_VALUE, null, ts))
        .set("arrdate").toDateArray(Arrays.asList(null, date))

        .set("nullbool").to((Boolean) null)
        .set("nullint64").to((Long) null)
        .set("nullfloat64").to((Double) null)
        .set("nullstring").to((String) null)
        .set("nullbytes").to((ByteArray) null)
        .set("nulltimestamp").to((Timestamp) null)
        .set("nulldate").to((Date) null)

        .set("nullarrbool").toBoolArray((Iterable<Boolean>) null)
        .set("nullarrint64").toInt64Array((Iterable<Long>) null)
        .set("nullarrfloat64").toFloat64Array((Iterable<Double>) null)
        .set("nullarrstring").toStringArray(null)
        .set("nullarrbytes").toBytesArray(null)
        .set("nullarrtimestamp").toTimestampArray(null)
        .set("nullarrdate").toDateArray(null);
  }

  private MutationGroup g(Mutation mutation, Mutation... other) {
    return MutationGroup.create(mutation, other);
  }

  private void serializeAndVerify(MutationGroup expected) {
    SpannerSchema schema = this.allTypesSchema;
    serializeAndVerify(expected, schema);
  }

  private static void serializeAndVerify(MutationGroup expected, SpannerSchema schema) {
    MutationGroupEncoder coder = new MutationGroupEncoder(schema);
    byte[] encode = coder.encode(expected);
    MutationGroup actual = coder.decode(encode);

    Assert.assertTrue(mutationGroupsEqual(expected, actual));
  }

  private static boolean mutationGroupsEqual(MutationGroup a, MutationGroup b) {
    ImmutableList<Mutation> alist = ImmutableList.copyOf(a);
    ImmutableList<Mutation> blist = ImmutableList.copyOf(b);

    if (alist.size() != blist.size()) {
      return false;
    }

    for (int i = 0; i < alist.size(); i++) {
      if (!mutationsEqual(alist.get(i), blist.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean mutationsEqual(Mutation a, Mutation b) {
    if (a == b) {
      return true;
    }
    if (b == null) {
      return false;
    }
    if (a.getOperation() != b.getOperation()) {
      return false;
    }
    if (!a.getTable().equalsIgnoreCase(b.getTable())) {
      return false;
    }
    if (a.getOperation() == Mutation.Op.DELETE) {
      return a.getKeySet().equals(b.getKeySet());
    }

    return ImmutableSet.copyOf(getNormalizedColumns(a))
        .equals(ImmutableSet.copyOf(getNormalizedColumns(b))) && ImmutableSet.copyOf(a.getValues())
        .equals(ImmutableSet.copyOf(b.getValues()));

  }

  // Pray for Java 8 support.
  private static Iterable<String> getNormalizedColumns(Mutation a) {
    return Iterables.transform(a.getColumns(), new Function<String, String>() {

      @Override
      public String apply(String input) {
        return input.toLowerCase();
      }
    });
  }
}
