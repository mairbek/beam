package org.apache.beam.sdk.io.gcp.spanner;

import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceFactory;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

import javax.annotation.concurrent.GuardedBy;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link SpannerIO}.
 */
@RunWith(JUnit4.class)
public class SpannerIOReadTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
  }

  @Test
  public void emptyTransform() throws Exception {
    SpannerIO.Read read = SpannerIO.read();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyInstanceId() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withDatabaseId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyDatabaseId() throws Exception {
    SpannerIO.Read read = SpannerIO.read().withInstanceId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires database id to be set with");
    read.validate(null);
  }

  @Test
  public void emptyTimestampBound() throws Exception {
    SpannerIO.Read read = SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires timestamp to be set");
    read.validate(null);
  }

  @Test
  public void emptyQuery() throws Exception {
    SpannerIO.Read read = SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("requires configuring query or read operation");
    read.validate(null);
  }

  @Test
  public void emptyColumns() throws Exception {
    SpannerIO.Read read = SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires a list of columns");
    read.validate(null);
  }

  @Test
  public void validRead() throws Exception {
    SpannerIO.Read read = SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withTable("users")
            .withColumns("id", "name", "email");
    read.validate(null);
  }

  @Test
  public void validQuery() throws Exception {
    SpannerIO.Read read = SpannerIO.read()
            .withInstanceId("123")
            .withDatabaseId("aaa")
            .withTimestamp(Timestamp.now())
            .withQuery("SELECT * FROM users");
    read.validate(null);
  }

}
