/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link SpannerIO}.
 */
@RunWith(JUnit4.class)
public class SizeBatchingTest implements Serializable {

  @Test
  public void batching() throws Exception {
    MutationGroup one = g(Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build());
    MutationGroup two = g(Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build());

    SizeBatchingFn writerFn = new SizeBatchingFn(1000000000);
    DoFnTester<MutationGroup, Iterable<MutationGroup>> fnTester = DoFnTester.of(writerFn);
    List<Iterable<MutationGroup>> batches = fnTester.processBundle(Arrays.asList(one, two));

    assertEquals(1, batches.size());
    assertEquals(2, countMutations(batches.get(0)));
  }

  @Test
  public void batchingGroups() throws Exception {
    MutationGroup one = g(Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build());
    MutationGroup two = g(Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build());
    MutationGroup three = g(Mutation.newInsertOrUpdateBuilder("test").set("three").to(3).build());

    // Have a room to accumulate one more item.
    long batchSize = MutationSizeEstimator.sizeOf(one) + 1;

    SizeBatchingFn writerFn = new SizeBatchingFn(batchSize);
    DoFnTester<MutationGroup, Iterable<MutationGroup>> fnTester = DoFnTester.of(writerFn);
    List<Iterable<MutationGroup>> batches = fnTester.processBundle(Arrays.asList(one, two, three));

    assertEquals(2, batches.size());
    assertEquals(2, countMutations(batches.get(0)));
    assertEquals(1, countMutations(batches.get(1)));
  }

  @Test
  public void noBatching() throws Exception {
    MutationGroup one = g(Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build());
    MutationGroup two = g(Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build());

    SizeBatchingFn writerFn = new SizeBatchingFn(0);
    DoFnTester<MutationGroup, Iterable<MutationGroup>> fnTester = DoFnTester.of(writerFn);
    List<Iterable<MutationGroup>> batches = fnTester.processBundle(Arrays.asList(one, two));

    assertEquals(2, batches.size());
    assertEquals(1, countMutations(batches.get(0)));
    assertEquals(1, countMutations(batches.get(1)));
  }

  @Test
  public void groups() throws Exception {
    Mutation one = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation two = Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build();
    Mutation three = Mutation.newInsertOrUpdateBuilder("test").set("three").to(3).build();

    // Smallest batch size

    SizeBatchingFn writerFn = new SizeBatchingFn( 1);
    DoFnTester<MutationGroup, Iterable<MutationGroup>> fnTester = DoFnTester.of(writerFn);
    List<Iterable<MutationGroup>> batches = fnTester.processBundle(
        Collections.singletonList(g(one, two, three)));

    assertEquals(1, batches.size());
    assertEquals(3, countMutations(batches.get(0)));
  }

  private static int countMutations(Iterable<MutationGroup> mutations) {
    int result = 0;
    for (MutationGroup g : mutations) {
      for (Mutation m : g) {
        result++;
      }
    }
    return result;
  }

  private static MutationGroup g(Mutation m, Mutation... other) {
    return MutationGroup.create(m, other);
  }
}
