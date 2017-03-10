package com.google.cloud.spanner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.spanner.v1.Mutation;

import java.io.Serializable;
import java.util.Map;

public class InsertOrUpdate implements Serializable {
    private final ImmutableMap<String, Value> values;

    private InsertOrUpdate(ImmutableMap<String, Value> values) {
        this.values = values;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static InsertOrUpdate fromProto(com.google.spanner.v1.Mutation.Write proto) {
        InsertOrUpdate.Builder builder = InsertOrUpdate.builder();

        for (int i = 0; i < proto.getColumnsCount(); i++) {
            // TODO(mairbek): google proto to spanner value???
            builder.set(proto.getColumns(i)).to(proto.getValues(0).getValues(i).getStringValue());
        }
        return builder.build();
    }

    public Map<String, Value> asMap() {
        return values;
    }

    com.google.spanner.v1.Mutation.Write toProto() {
        com.google.spanner.v1.Mutation.Write.Builder builder = Mutation.Write.newBuilder();
        ListValue.Builder values = builder.addValuesBuilder();
        for (Map.Entry<String, Value> kv :
                this.values.entrySet()) {
            builder.addColumns(kv.getKey());
            values.addValues(kv.getValue().toProto());
        }
        return builder.build();
    }

    public static class Builder {
        private ImmutableMap.Builder<String, Value> values = ImmutableMap.builder();
        private String currentColumn;
        private ValueBinder<Builder> valueBinder;

        private Builder() {
            valueBinder = new ValueBinder<Builder>() {
                @Override
                Builder handle(Value value) {
                    Preconditions.checkNotNull(value);
                    Preconditions.checkNotNull(currentColumn);
                    values.put(currentColumn, value);
                    currentColumn = null;
                    return Builder.this;
                }
            };
        }

        public ValueBinder<Builder> set(String column) {
            currentColumn = column;
            return this.valueBinder;
        }

        public InsertOrUpdate build() {
            return new InsertOrUpdate(values.build());
        }

    }
}
