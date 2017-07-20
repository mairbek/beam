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

import com.google.cloud.spanner.AbortedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Batches together and writes mutations to Google Cloud Spanner. */
@VisibleForTesting
class SpannerWriteGroupFn extends AbstractSpannerFn<Iterable<MutationGroup>,
    Void> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerWriteGroupFn.class);
  private final SpannerConfig spannerConfig;

  private static final int MAX_RETRIES = 5;
  private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
      FluentBackoff.DEFAULT
          .withMaxRetries(MAX_RETRIES)
          .withInitialBackoff(Duration.standardSeconds(5));

  @VisibleForTesting SpannerWriteGroupFn(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return spannerConfig;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();

    while (true) {
      // Batch upsert rows.
      try {
        databaseClient().writeAtLeastOnce(Iterables.concat(c.element()));

        // Break if the commit threw no exception.
        break;
      } catch (AbortedException exception) {
        // Only log the code and message for potentially-transient errors. The entire exception
        // will be propagated upon the last retry.
        LOG.error(
            "Error writing to Spanner ({}): {}", exception.getCode(), exception.getMessage());
        if (!BackOffUtils.next(sleeper, backoff)) {
          LOG.error("Aborting after {} retries.", MAX_RETRIES);
          throw exception;
        }
      }
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    spannerConfig.populateDisplayData(builder);
  }
}
