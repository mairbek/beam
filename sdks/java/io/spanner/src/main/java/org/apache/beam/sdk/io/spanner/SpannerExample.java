package org.apache.beam.sdk.io.spanner;

import com.google.cloud.spanner.InsertOrUpdate;
import com.google.cloud.spanner.InsertOrUpdateCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.UUID;

public class SpannerExample {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);
        // TODO(mairbek): better way to do this?
        p.getCoderRegistry().registerCoder(InsertOrUpdate.class,
                InsertOrUpdateCoder.class);

        PCollection<String> lines = p.apply(
                "ReadLines", TextIO.Read.from("/Users/mairbek/random.txt"));

        PCollection<InsertOrUpdate> mutations = lines.apply("Mutate", ParDo.of(new DoFn<String,
                InsertOrUpdate>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String val = c.element();
                InsertOrUpdate mutation = InsertOrUpdate.builder().set("key").to(generateKey()).set("name").to(val).build();
                c.output(mutation);
            }
        }));

        mutations.apply(SpannerIO.write("span-cloud-testing", "mairbek-df", "mydb", "users"));

        p.run().waitUntilFinish();
    }

    private static String generateKey() {
        return UUID.randomUUID().toString();
    }

}
