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

import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerReadIT {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  /** Pipeline options for this test. */
  public interface SpannerTestPipelineOptions extends TestPipelineOptions {
    @Description("Project ID for Spanner")
    @Default.String("span-cloud-testing")
    String getProjectId();
    void setProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("mairbek-df")
    String getInstanceId();
    void setInstanceId(String value);

    @Description("Database ID to write to in Spanner")
    @Default.String("mydb")
    String getDatabaseId();

    void setDatabaseId(String value);

    @Description("Table name")
    @Default.String("users")
    String getTable();
    void setTable(String value);
  }

  private Spanner spanner;
  private DatabaseAdminClient databaseAdminClient;
  private SpannerTestPipelineOptions options;

  @Before
  public void setUp() throws Exception {
    PipelineOptionsFactory.register(SpannerTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SpannerTestPipelineOptions.class);

    spanner = SpannerOptions.newBuilder().setProjectId(options.getProjectId()).build().getService();

    databaseAdminClient = spanner.getDatabaseAdminClient();

    // Delete database if exists.
    databaseAdminClient.dropDatabase(options.getInstanceId(), options.getDatabaseId());

    Operation<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabase(
            options.getInstanceId(),
            options.getDatabaseId(),
            Collections.singleton(
                "CREATE TABLE "
                    + options.getTable()
                    + " ("
                    + "  Key           INT64,"
                    + "  Value         STRING(MAX),"
                    + ") PRIMARY KEY (Key)"));
    op.waitFor();
  }

  @Test
  public void testWrite() throws Exception {
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(
            DatabaseId.of(
                options.getProjectId(), options.getInstanceId(), options.getDatabaseId()));

    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < 5L; i++) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder(options.getTable())
              .set("key")
              .to((long) i)
              .set("value")
              .to(RandomStringUtils.random(100, true, true))
              .build());
    }

    databaseClient.writeAtLeastOnce(mutations);

    PCollection<Struct> output =
        p.apply(SpannerIO.read().withQuery("SELECT * FROM " + options.getTable()));
    PAssert.thatSingleton(output.apply("Count rows", Count.<Struct>globally())).isEqualTo(5L);
    p.run();
  }

  @After
  public void tearDown() throws Exception {
    databaseAdminClient.dropDatabase(options.getInstanceId(), options.getDatabaseId());
    spanner.closeAsync().get();
  }
}
