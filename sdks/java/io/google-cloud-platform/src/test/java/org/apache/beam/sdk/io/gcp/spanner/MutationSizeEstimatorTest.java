package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MutationSizeEstimatorTest {

    @Test
    public void primitives() throws Exception {
        Mutation int64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
        Mutation float64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(2.9).build();
        Mutation bool = Mutation.newInsertOrUpdateBuilder("test").set("one").to(false).build();

        assertThat(MutationSizeEstimator.sizeOf(int64), is(8L));
        assertThat(MutationSizeEstimator.sizeOf(float64), is(8L));
        assertThat(MutationSizeEstimator.sizeOf(bool), is(1L));
    }

    @Test
    public void primitiveArrays() throws Exception {
        Mutation int64 = Mutation.newInsertOrUpdateBuilder("test").set("one").toInt64Array(new
                        long[]{1L, 2L, 3L})
                .build();
        Mutation float64 = Mutation.newInsertOrUpdateBuilder("test").set("one").toFloat64Array(new
                double[]{1., 2.}).build();
        Mutation bool = Mutation.newInsertOrUpdateBuilder("test").set("one").toBoolArray(new
                boolean[] {true, true, false, true})
                .build();

        assertThat(MutationSizeEstimator.sizeOf(int64), is(24L));
        assertThat(MutationSizeEstimator.sizeOf(float64), is(16L));
        assertThat(MutationSizeEstimator.sizeOf(bool), is(4L));
    }

    @Test
    public void strings() throws Exception {
        Mutation emptyString = Mutation.newInsertOrUpdateBuilder("test").set("one").to("").build();
        Mutation nullString = Mutation.newInsertOrUpdateBuilder("test").set("one").to((String)null)
                .build();
        Mutation sampleString = Mutation.newInsertOrUpdateBuilder("test").set("one").to("abc")
                .build();
        Mutation sampleArray = Mutation.newInsertOrUpdateBuilder("test").set("one")
                .toStringArray(Arrays.asList("one", "two", null))
                .build();

        assertThat(MutationSizeEstimator.sizeOf(emptyString), is(0L));
        assertThat(MutationSizeEstimator.sizeOf(nullString), is(0L));
        assertThat(MutationSizeEstimator.sizeOf(sampleString), is(3L));
        assertThat(MutationSizeEstimator.sizeOf(sampleArray), is(6L));
    }

    @Test
    public void bytes() throws Exception {
        Mutation empty = Mutation.newInsertOrUpdateBuilder("test").set("one").to(ByteArray
                .fromBase64(""))
                .build();
        Mutation nullValue = Mutation.newInsertOrUpdateBuilder("test").set("one").to((ByteArray)null)
                .build();
        Mutation sample = Mutation.newInsertOrUpdateBuilder("test").set("one").to(ByteArray
                .fromBase64("abcdabcd"))
                .build();

        assertThat(MutationSizeEstimator.sizeOf(empty), is(0L));
        assertThat(MutationSizeEstimator.sizeOf(nullValue), is(0L));
        assertThat(MutationSizeEstimator.sizeOf(sample), is(6L));
    }

    @Test
    public void dates() throws Exception {
        Mutation timestamp = Mutation.newInsertOrUpdateBuilder("test").set("one").to(Timestamp
                .now())
                .build();
        Mutation nullTimestamp = Mutation.newInsertOrUpdateBuilder("test").set("one").to(
                (Timestamp)null)
                .build();
        Mutation date = Mutation.newInsertOrUpdateBuilder("test").set("one").to(Date
                .fromYearMonthDay(2017, 10, 10))
                .build();
        Mutation nullDate = Mutation.newInsertOrUpdateBuilder("test").set("one").to(
                (Date)null)
                .build();
        Mutation timestampArray = Mutation.newInsertOrUpdateBuilder("test").set("one")
                .toTimestampArray(Arrays.asList(Timestamp
                        .now(), null))
                .build();
        Mutation dateArray = Mutation.newInsertOrUpdateBuilder("test").set("one")
                .toDateArray(Arrays.asList(null, Date.fromYearMonthDay(2017, 1, 1), null, Date
                        .fromYearMonthDay(2017, 1, 2)))
                .build();


        assertThat(MutationSizeEstimator.sizeOf(timestamp), is(12L));
        assertThat(MutationSizeEstimator.sizeOf(date), is(12L));
        assertThat(MutationSizeEstimator.sizeOf(nullTimestamp), is(12L));
        assertThat(MutationSizeEstimator.sizeOf(nullDate), is(12L));
        assertThat(MutationSizeEstimator.sizeOf(timestampArray), is(24L));
        assertThat(MutationSizeEstimator.sizeOf(dateArray), is(48L));
    }


}
