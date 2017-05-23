package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class StructCoderTest {

    private StructCoder coder = new StructCoder();

    @Test
    public void empty() throws Exception {
        verifyCoder(Struct.newBuilder().build());
    }

    @Test
    public void nulls() throws Exception {
        verifyCoder(Struct.newBuilder()
                .set("bool").to((Boolean) null)
                .set("int").to((Long) null)
                .set("float").to((Double) null)
                .set("str").to((String) null)
                .set("byte").to((ByteArray) null)
                .set("timestamp").to((Timestamp) null)
                .set("date").to((Date) null)
                .build());
    }

    @Test
    public void primitiveValues() throws Exception {
        verifyCoder(Struct.newBuilder()
                .set("bool").to(true)
                .set("int").to(1L)
                .set("float").to(1.1)
                .set("str").to("StringValue")
                .set("byte").to(ByteArray.copyFrom(new byte[]{1, 2, 3, 4, 5}))
                .set("timestamp").to(Timestamp.MIN_VALUE)
                .set("date").to(Date.fromYearMonthDay(2017, 12, 12))
                .build());
    }

    @Test
    public void arrayValues() throws Exception {
        verifyCoder(Struct.newBuilder()
                .set("bools").toBoolArray(Arrays.asList(true, null, false, null, true))
                .set("ints").toInt64Array(Arrays.asList(1L, 2L, null, 4L))
                .set("floats").toFloat64Array(Arrays.asList(1.1, null, 2.2, null, 4.5))
                .set("strings").toStringArray(Arrays.asList(null, "one", "five", null))
                .set("bytes").toBytesArray(Arrays.asList(ByteArray.fromBase64("aab"), null,
                        ByteArray.fromBase64("bbcw")))
                .set("timestamps").toTimestampArray(Arrays.asList(Timestamp.MAX_VALUE, null,
                        Timestamp.MIN_VALUE))
                .set("dates").toDateArray(Arrays.asList(Date.fromYearMonthDay(1, 1, 1), null))
                .build());
    }

    @Test
    public void arrayNulls() throws Exception {
        verifyCoder(Struct.newBuilder()
                .set("bools").toBoolArray((boolean[]) null)
                .set("ints").toInt64Array((Iterable<Long>) null)
                .set("floats").toFloat64Array((double[]) null)
                .set("strings").toStringArray(null)
                .set("bytes").toBytesArray(null)
                .set("timestamps").toTimestampArray(null)
                .set("dates").toDateArray(null)
                .build());
    }

    @Test
    public void nestedStruct() throws Exception {
        List<Type.StructField> fieldTypes =
                Arrays.asList(
                        Type.StructField.of("ff1", Type.string()), Type.StructField.of("ff2", Type.int64()));
        List<Struct> arrayElements =
                Arrays.asList(
                        Struct.newBuilder().set("ff1").to("v1").set("ff2").to(1).build(),
                        Struct.newBuilder().set("ff1").to((String) null).set("ff2").to(10).build(),
                        Struct.newBuilder().set("ff1").to("v2").set("ff2").to((Long) null).build(),
                        Struct.newBuilder().set("ff1").to((String) null).set("ff2").to((Long) null).build());
        Struct struct =
                Struct.newBuilder().set("f1").to("x").add("f2", fieldTypes, arrayElements).build();
        verifyCoder(struct);

    }

    private void verifyCoder(Struct expected) throws org.apache.beam.sdk.coders.CoderException {
        byte[] val = CoderUtils.encodeToByteArray(coder, expected);
        Struct actual = CoderUtils.decodeFromByteArray(coder, val);
        Assert.assertThat(actual, equalTo(expected));
    }
}
