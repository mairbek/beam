package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Comparator;

// TODO(mairbek): Document
public class KeyComparator implements Comparator<MutationGroup>, Serializable {
    private final ImmutableList<String> fields;
    private final ImmutableList<Ordering> orderings;

    private KeyComparator(ImmutableList<String> fields, ImmutableList<Ordering> orderings) {
        this.fields = fields;
        this.orderings = orderings;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static int compareValues(Value v1, Value v2) {
        if (v1.isNull()) {
            return v2.isNull() ? 0 : 1;
        }
        if (!v1.getType().equals(v2.getType())) {
            throw new IllegalArgumentException("value type is different");
        }
        switch (v1.getType().getCode()) {
            case BOOL:
                return Boolean.compare(v1.getBool(), v2.getBool());
            case INT64:
                return Long.compare(v1.getInt64(), v2.getInt64());
            case FLOAT64:
                return Double.compare(v1.getFloat64(), v2.getFloat64());
            case STRING:
                return v1.getString().compareTo(v2.getString());
            // TODO(mairbek): support other?
        }
        throw new IllegalArgumentException("Unsupported primary key type " + v1.getType());
    }

    @Override
    public int compare(MutationGroup a, MutationGroup b) {
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            Ordering ordering = orderings.get(i);
            // TODO(mairbek): deletes!
            int compare = compareValues(a.primary().asMap().get(field), b.primary().asMap().get(field));
            if (compare != 0) {
                return ordering.getMultiplier() * compare;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyComparator that = (KeyComparator) o;

        if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;
        return orderings != null ? orderings.equals(that.orderings) : that.orderings == null;
    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + (orderings != null ? orderings.hashCode() : 0);
        return result;
    }

    private enum Ordering implements Serializable {
        ASC {
            @Override
            int getMultiplier() {
                return 1;
            }
        }, DESC {
            @Override
            int getMultiplier() {
                return -1;
            }
        };

        abstract int getMultiplier();
    }

    public static class Builder {
        private final ImmutableList.Builder<String> fields = ImmutableList.builder();
        private final ImmutableList.Builder<Ordering> orderings = ImmutableList.builder();

        private Builder() {
        }

        public Builder asc(String field) {
            fields.add(field);
            orderings.add(Ordering.ASC);
            return this;
        }

        public Builder desc(String field) {
            fields.add(field);
            orderings.add(Ordering.DESC);
            return this;
        }

        public KeyComparator build() {
            return new KeyComparator(fields.build(), orderings.build());
        }

    }
}
