package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;

/**
 * Estimates the size of {@link com.google.cloud.spanner.Mutation}. Only checks value size,
 * and doesn't take gRPC serialization overhead into account.
 */
public class MutationSizeEstimator {

    private MutationSizeEstimator() {
    }

    public static long sizeOf(Mutation m) {
        long result = 0;
        for (Value v : m.getValues()) {
            switch (v.getType().getCode()) {
                case ARRAY:
                    result += estimateArrayValue(v);
                    break;
                case STRUCT:
                    throw new IllegalArgumentException("Structs are not supported in mutation.");
                default:
                    result += estimatePrimitiveValue(v);
            }
        }

        return result;
    }
    public static long sizeOf(MutationGroup group) {
        long result = 0;
        for (Mutation m : group.allMutations()) {
            result += sizeOf(m);
        }
        return result;
    }

    public static long estimatePrimitiveValue(Value v) {
        if (v.isNull()) {
            return 0;
        }
        switch (v.getType().getCode()) {
            case BOOL:
                return 1;
            case INT64:
            case FLOAT64:
                return 64;
            case STRING:
                return 8 * v.getString().length();
            case BYTES:
                return 8 * 4 * v.getBytes().length() / 3;
            case DATE:
                // A string of form "2016-09-15", 10 chars
                return 80;
            case TIMESTAMP:
                // A string of form "2016-09-15T00:00:00Z", 20 chars.
                return 160;
        }
        throw new IllegalArgumentException("Unsupported type " + v.getType());
    }

    public static long estimateArrayValue(Value v) {
        switch (v.getType().getArrayElementType().getCode()) {
            case BOOL:
                return v.getBoolArray().size();
            case INT64:
                return 64 * v.getInt64Array().size();
            case FLOAT64:
                return 64 * v.getFloat64Array().size();
            case STRING:
                long totalLength = 0;
                for (String s : v.getStringArray()) {
                    if (s == null) {
                        continue;
                    }
                    totalLength += s.length();
                }
                return 8 * totalLength;
            case BYTES:
                totalLength = 0;
                for (ByteArray bytes : v.getBytesArray()) {
                    if (bytes == null) {
                        continue;
                    }
                    totalLength += bytes.length();
                }
                return 8 * totalLength * 4 / 3;
            case DATE:
                return 80 * v.getDateArray().size();
            case TIMESTAMP:
                return 160 * v.getDateArray().size();
        }
        throw new IllegalArgumentException("Unsupported type " + v.getType());
    }

}
