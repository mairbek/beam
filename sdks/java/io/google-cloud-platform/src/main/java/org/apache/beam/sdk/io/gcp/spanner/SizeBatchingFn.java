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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;

import java.util.ArrayList;
import java.util.List;

public class SizeBatchingFn extends DoFn<MutationGroup, Iterable<MutationGroup>> {
  private final long maxBatchSizeBytes;
  // Current batch of mutations to be written.
  private List<MutationGroup> mutations;
  private long batchSizeBytes = 0;


  public SizeBatchingFn(long maxBatchSizeBytes) {
    this.maxBatchSizeBytes = maxBatchSizeBytes;
  }

  @Setup
  public void setup() throws Exception {
    mutations = new ArrayList<>();
    batchSizeBytes = 0;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    MutationGroup m = c.element();
    mutations.add(m);
    batchSizeBytes += MutationSizeEstimator.sizeOf(m);
    if (batchSizeBytes >= maxBatchSizeBytes) {
      c.output(mutations);
      mutations = new ArrayList<>();
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    if (!mutations.isEmpty()) {
      c.output(mutations, GlobalWindow.INSTANCE.maxTimestamp(), GlobalWindow.INSTANCE);
    }
  }
}
