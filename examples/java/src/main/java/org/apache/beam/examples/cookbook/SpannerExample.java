package org.apache.beam.examples.cookbook;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationCoder;
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
                "ReadLines", TextIO.Read.from("/tmp/input.txt"));

        PCollection<Mutation> mutations = lines.apply("Mutate", ParDo.of(new DoFn<String, Mutation>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String val = c.element();
                long id = UUID.randomUUID().getMostSignificantBits();
                Mutation mutation = Mutation.newInsertOrUpdateBuilder("users").set("id").to(id).set("name").to(val).build();
                c.output(mutation);
            }
        })).setCoder(MutationCoder.of());

        mutations.apply(SpannerIO.write("span-cloud-testing", "mairbek-df", "users"));

        p.run().waitUntilFinish();
    }

}
