package com.google.cloud.spanner;

import com.google.spanner.v1.Mutation;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public final class InsertOrUpdateCoder extends AtomicCoder<InsertOrUpdate> implements Serializable {
    private static final InsertOrUpdateCoder INSTANCE = new InsertOrUpdateCoder();
    private final ProtoCoder<Mutation.Write> protoCoder = ProtoCoder.of(com.google.spanner
            .v1.Mutation.Write.class);

    private InsertOrUpdateCoder() {

    }

    public static InsertOrUpdateCoder of() {
        return INSTANCE;
    }

    @Override
    public void encode(InsertOrUpdate value, OutputStream outStream, Context context) throws IOException {
        protoCoder.encode(value.toProto(), outStream, context);
    }

    @Override
    public InsertOrUpdate decode(InputStream inStream, Context context) throws CoderException, IOException {
        Mutation.Write proto = protoCoder.decode(inStream, context);
        return InsertOrUpdate.fromProto(proto);
    }
}
