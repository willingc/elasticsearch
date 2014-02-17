package org.elasticsearch.search.aggregations.metrics.percentile.tdigest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.metrics.percentile.PercentilesEstimator;

import java.io.IOException;
import java.util.Map;


public class TDigest extends PercentilesEstimator {

    public final static byte ID = 1;

    public ObjectArray<TDigestState> states;
    private final double compression;

    public TDigest(double[] percents, double compression, long estimatedBucketsCount) {
        super(percents);
        states = BigArrays.newObjectArray(estimatedBucketsCount);
        this.compression = compression;
    }

    public void offer(double value, long bucketOrd) {
        if (bucketOrd >= states.size()) {
            long overSize = BigArrays.overSize(bucketOrd + 1);
            states = BigArrays.resize(states, overSize);
        }
        TDigestState state = states.get(bucketOrd);
        if (state == null) {
            state = new TDigestState(compression);
            states.set(bucketOrd, state);
        }
        state.add(value);
    }

    @Override
    public Result flyweight(long bucketOrd) {
        return new Flyweight(percents, states.get(bucketOrd));
    }

    public long ramBytesUsed() {
        return -1;
    }

    @Override
    public Result emptyFlyweight() {
        return new Flyweight(percents, new TDigestState(compression));
    }

    public static class Flyweight extends Result<TDigest, Flyweight> {

        private TDigestState state;

        public Flyweight() {} // for serialization

        public Flyweight(double[] percents, TDigestState state) {
            super(percents);
            this.state = state;
        }

        @Override
        protected byte id() {
            return ID;
        }

        @Override
        public double estimate(int index) {
            return state == null || state.size() > 0 ? Double.NaN : state.quantile(percents[index] / 100);
        }

        @Override
        public Merger merger(int estimatedMerges) {
            return new Merger();
        }

        public static Flyweight read(StreamInput in) throws IOException {
            Flyweight flyweight = new Flyweight();
            flyweight.readFrom(in);
            return flyweight;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.percents = new double[in.readInt()];
            for (int i = 0; i < percents.length; i++) {
                percents[i] = in.readDouble();
            }
            state = TDigestState.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(percents.length);
            for (int i = 0 ; i < percents.length; ++i) {
                out.writeDouble(percents[i]);
            }
            TDigestState.write(state, out);
        }

        private class Merger implements Result.Merger<TDigest, Flyweight> {

            private Flyweight merged;

            @Override
            public void add(Flyweight flyweight) {
                if (merged == null || merged.state == null) {
                    merged = flyweight;
                    return;
                }
                if (flyweight.state == null || flyweight.state.size() == 0) {
                    return;
                }
                merged.state.add(flyweight.state);
            }

            @Override
            public Flyweight merge() {
                return merged;
            }
        }

    }

    public static class Factory implements PercentilesEstimator.Factory {

        private final double compression;

        public Factory(Map<String, Object> settings) {
            double compression = 100;
            if (settings != null) {
                Double value = (Double) settings.get("compression");
                if (value != null) {
                    compression = value;
                }
            }
            this.compression = compression;
        }

        public TDigest create(double[] percents, long estimtedBucketCount) {
            return new TDigest(percents, compression, estimtedBucketCount);
        }
    }

}