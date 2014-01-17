package org.elasticsearch.search.aggregations.metrics.percentile.tdigest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.percentile.InternalPercentiles;

import java.io.IOException;
import java.util.Map;


public class TDigest extends InternalPercentiles.Estimator {

    public final static byte ID = 2;

    public TDigestState state;

    public TDigest() {} // for serialization

    public TDigest(double[] percents, double compression) {
        super(percents);
        state = new TDigestState(compression);
    }

    @Override
    protected byte id() {
        return ID;
    }

    public void offer(double value) {
        state.add(value);
    }


    public double estimate(int index) {
        return state.quantile(percents[index] / 100);
    }

    @Override
    public InternalPercentiles.Estimator.Merger merger(int expectedMerges) {
        return new Merger();
    }

    public long ramBytesUsed() {
        return -1;
    }

    public static TDigest readNewFrom(StreamInput in) throws IOException {
        TDigest digest = new TDigest();
        digest.readFrom(in);
        return digest;
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

    private class Merger implements InternalPercentiles.Estimator.Merger<TDigest> {

        private TDigest merged;

        @Override
        public void add(TDigest tDigest) {
            if (merged == null) {
                merged = tDigest;
                return;
            }
            if (tDigest.state.size() == 0) {
                return;
            }
            merged.state.add(tDigest.state);
        }

        @Override
        public TDigest merge() {
            return merged;
        }
    }

    public static class Factory implements InternalPercentiles.Estimator.Factory {

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

        public TDigest create(double[] percents) {
            return new TDigest(percents, compression);
        }
    }

}