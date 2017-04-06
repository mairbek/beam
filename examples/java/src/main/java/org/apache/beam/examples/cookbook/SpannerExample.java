package org.apache.beam.examples.cookbook;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
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

        PCollection<String> lines = p.apply(
                "ReadLines", TextIO.Read.from("/Users/mairbek/input.txt"));

        PCollection<Mutation> mutations = lines.apply("Mutate", ParDo.of(new DoFn<String,
                Mutation>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String val = c.element();
                Mutation mutation = Mutation.newInsertOrUpdateBuilder("users").set("key").to
                        (generateKey()).set("name")
                        .to(val).build();
                c.output(mutation);
            }
        }));

        System.out.println("mutate");

        mutations.apply(SpannerIO.writeTo("span-cloud-testing", "mairbek-df", "mydb"));

        p.run().waitUntilFinish();
    }

    private static String generateKey() {
        return UUID.randomUUID().toString();
    }

}
