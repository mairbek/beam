package com.google.cloud.spanner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

public class InsertOrUpdate implements Serializable {
    private final ImmutableMap<String, Value> values;

    private InsertOrUpdate(ImmutableMap<String, Value> values) {
        this.values = values;
    }

    public Map<String, Value> asMap() {
        return values;
    }

    public static class Builder {
        private ImmutableMap.Builder<String, Value> values = ImmutableMap.builder();
        private String currentColumn;
        private ValueBinder<Builder> valueBinder;

        public Builder() {
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

        Builder set(String column) {
            currentColumn = column;
            return this;
        }

        InsertOrUpdate build() {
            return new InsertOrUpdate(values.build());
        }

    }

}
