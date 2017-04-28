package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A bundle of mutations that must be submitted atomically.
 *
 * One of the mutations is chosen to be "primary", and is used in determining partitions.
 */
public final class MutationGroup implements Serializable, Iterable<Mutation> {
    private final ImmutableList<Mutation> mutations;

    public static Builder withPrimary(Mutation primary) {
        return new Builder(primary);
    }

    @Override
    public Iterator<Mutation> iterator() {
        return mutations.iterator();
    }

    public static class Builder {
        private final ImmutableList.Builder<Mutation> builder;

        private Builder(Mutation primary) {
            this.builder = ImmutableList.<Mutation>builder().add(primary);
        }

        public Builder attach(Mutation m) {
            this.builder.add(m);
            return this;
        }

        public Builder attach(Iterable<Mutation> mutations) {
            this.builder.addAll(mutations);
            return this;
        }

        public Builder attach(Mutation... mutations) {
            this.builder.addAll(Arrays.asList(mutations));
            return this;
        }

        public MutationGroup build() {
            return new MutationGroup(builder.build());
        }
    }

    private MutationGroup(ImmutableList<Mutation> mutations) {
        this.mutations = mutations;
    }

    public Mutation primary() {
        return mutations.get(0);
    }

    public List<Mutation> attached() {
        return mutations.subList(1, mutations.size());
    }
}
