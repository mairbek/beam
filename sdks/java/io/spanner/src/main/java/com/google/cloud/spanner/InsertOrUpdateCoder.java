package com.google.cloud.spanner;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class InsertOrUpdateCoder extends AtomicCoder<InsertOrUpdate> implements Serializable {
    @Override
    public void encode(InsertOrUpdate value, OutputStream outStream, Context context) throws CoderException, IOException {

    }

    @Override
    public InsertOrUpdate decode(InputStream inStream, Context context) throws CoderException, IOException {
        return null;
    }
}
