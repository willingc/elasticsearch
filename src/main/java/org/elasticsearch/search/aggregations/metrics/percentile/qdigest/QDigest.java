package org.elasticsearch.search.aggregations.metrics.percentile.qdigest;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.percentile.InternalPercentiles;

import java.io.IOException;
import java.util.Map;

public class QDigest extends InternalPercentiles.Estimator<QDigest> {

    public final static byte ID = 1;

    public QDigestState state;

    private QDigest() {} // for serialization

    @Override
    protected byte id() {
        return ID;
    }

    public QDigest(double[] percents, double compression) {
        super(percents);
        state = new QDigestState(compression);
    }

    public void offer(double value) {
        state.offer((long) value);
    }

    public double estimate(int index) {
        return !state.isEmpty() ? state.getQuantile(percents[index] / 100) : Double.NaN;
    }

    @Override
    public Merger merger(int expectedMerges) {
        return new Merger();
    }

    public long ramBytesUsed() {
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF + state.ramBytesUsed() +
               RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + percents.length * 8;
    }

    public static QDigest readNewFrom(StreamInput in) throws IOException {
        QDigest digest = new QDigest();
        digest.readFrom(in);
        return digest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.percents = new double[in.readInt()];
        for (int i = 0; i < percents.length; i++) {
            percents[i] = in.readDouble();
        }
        state = QDigestState.read(in);
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(percents.length);
        for (int i = 0 ; i < percents.length; ++i) {
            out.writeDouble(percents[i]);
        }
        QDigestState.write(state, out);
    }

    public static class Merger implements InternalPercentiles.Estimator.Merger<QDigest> {

        private QDigest reduced;

        @Override
        public void add(QDigest qDigest) {
            if (reduced == null) {
                reduced = qDigest;
                return;
            }
            if (reduced.state.isEmpty()) {
                return;
            }
            reduced.state = QDigestState.unionOf(reduced.state, qDigest.state);
        }

        @Override
        public QDigest merge() {
            return reduced;
        }
    }

    public static class Factory implements InternalPercentiles.Estimator.Factory<QDigest> {

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

        public QDigest create(double[] percents) {
            return new QDigest(percents, compression);
        }
    }

}