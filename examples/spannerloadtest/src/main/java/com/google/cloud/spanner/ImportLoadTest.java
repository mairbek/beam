package com.google.cloud.spanner;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.Collections;
import java.util.Random;

public class ImportLoadTest implements Serializable {

  public interface Options extends PipelineOptions {
    @Description("Create a sample database")
    @Default.Boolean(true)
    boolean isCreateDatabase();

    void setCreateDatabase(boolean createDatabase);

    @Description("Instance ID to write to in Spanner")
    @Validation.Required
    @Default.String("mairbek-omg")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID to write to in Spanner")
    @Validation.Required
    @Default.String("testdb")
    String getDatabaseId();

    void setDatabaseId(String databaseId);

    @Description("Table name")
    @Validation.Required
    @Default.String("users")
    String getTable();

    void setTable(String value);

    @Description("Number of shards to generate mutations")
    @Default.Integer(1000)
    Integer getNumberOfShards();
    void setNumberOfShards(Integer numberOfShards);

    @Description("Number of mutations per shards")
    @Default.Integer(100)
    Integer getMutationsPerShard();
    void setMutationsPerShard(Integer mutationsPerShard);


    @Description("Number of fields in the mutation table")
    @Default.Integer(10)
    Integer getNumberOfFields();
    void setNumberOfFields(Integer numberOfShards);

    @Description("Size of the field in bytes")
    @Default.Integer(100)
    Integer getFieldSize();

    void setFieldSize(Integer fieldSize);

  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    if (options.isCreateDatabase()) {
      createDatabase(options);
    }
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> lines = p.apply(Create.of(ContiguousSet.create(Range.closed(1, options
            .getNumberOfShards()),
        DiscreteDomain.integers
            ())));
    PCollection<Mutation> mutations = lines.apply(ParDo
        .of(new GenerateMutations(options.getTable(), options.getMutationsPerShard(),
            options.getNumberOfFields(), options.getFieldSize())));

    mutations.apply(SpannerIO.write().withSpannerConfig(
        SpannerConfig.create()
            .withHost("https://staging-wrenchworks.sandbox.googleapis.com")
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId())));
    p.run().waitUntilFinish();
  }

  private static void createDatabase(Options options) {
   SpannerOptions spannerOptions = SpannerOptions.newBuilder()
       .setHost("https://staging-wrenchworks.sandbox.googleapis.com").build();
    Spanner client = spannerOptions.getService();

    DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
    try {
      databaseAdminClient.dropDatabase(options.getInstanceId(), options.getDatabaseId());
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }
    StringBuilder ddl = new StringBuilder(
        "CREATE TABLE " + options.getTable() + " (" + "  Key           INT64,");
    for (int i = 0; i < options.getNumberOfFields(); i++) {
      ddl.append("  field").append(i).append(" STRING(MAX),");
    }
    ddl.append(" ) PRIMARY KEY (Key)");
    Operation<Database, CreateDatabaseMetadata> op = databaseAdminClient
        .createDatabase(options.getInstanceId(), options.getDatabaseId(),
            Collections.singleton(ddl.toString()));
    op.waitFor();
    client.close();
  }

  private static class GenerateMutations extends DoFn<Integer, Mutation> {

    private final String table;
    private final int numMutations;
    private final int numFields;
    private final int size;

    public GenerateMutations(String table, int numMutations, int numFields, int size) {
      this.table = table;
      this.numMutations = numMutations;
      this.numFields = numFields;
      this.size = size;
    }

    private static final char[] ALPHANUMERIC = "1234567890abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static String randomAlphaNumeric(int length) {
      Random random = new Random();
      char[] result = new char[length];
      for (int i = 0; i < length; i++) {
        result[i] = ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)];
      }
      return new String(result);
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
      Integer shard = c.element();
      for (int i = 0; i < numMutations; i++) {
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
        long key = ((long) shard) * numMutations + i;
        builder.set("Key").to(key);
        for (int field = 0; field < numFields; field++) {
          builder.set("Field" + field).to(randomAlphaNumeric(size));
        }
        Mutation mutation = builder.build();
        c.output(mutation);
      }
    }

  }

}
