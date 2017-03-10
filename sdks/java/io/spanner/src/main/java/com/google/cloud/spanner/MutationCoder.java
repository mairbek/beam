package com.google.cloud.spanner;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


// TODO(mairbek): we need something more reasonable
public class MutationCoder extends AtomicCoder<Mutation> implements Serializable {
    private static final MutationCoder INSTANCE = new MutationCoder();

    public static MutationCoder of() {
        return INSTANCE;
    }

    private final ProtoCoder<com.google.spanner.v1.Mutation> protoCoder = ProtoCoder.of(com.google.spanner.v1.Mutation.class);


    @Override
    public void encode(Mutation mutation, OutputStream outStream, Context context) throws CoderException, IOException {
        List<com.google.spanner.v1.Mutation> out = new ArrayList<>(1);
        Mutation.toProto(Collections.singleton(mutation), out);
        com.google.spanner.v1.Mutation proto = out.get(0);
        protoCoder.encode(proto, outStream, context);
    }

    @Override
    public Mutation decode(InputStream inStream, Context context) throws CoderException, IOException {
        com.google.spanner.v1.Mutation proto = protoCoder.decode(inStream, context);
        com.google.spanner.v1.Mutation.Write write = proto.getInsertOrUpdate();

        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(write.getTable());

        for (int i = 0; i < write.getColumnsCount(); i++) {
            // TODO(mairbek): google proto to spanner proto???
            builder.set(write.getColumns(i)).to(write.getValues(0).getValues(i).getStringValue());
        }

        Mutation result = builder.build();
        return result;
    }
}