package org.elasticsearch.search.aggregations.metrics.percentile.frugal;

import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.percentile.InternalPercentiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class Frugal extends InternalPercentiles.Estimator<Frugal> {

    public final static byte ID = 0;

    private final Random rand;

    public double[] estimates;  // Current estimate of percentile
    private int[] steps;        // Current step value for frugal-2u
    private OpenBitSet signs;   // Direction of last movement

    Frugal() { // for serialization

        // if we wanted to do it right... we'd need an "OpenRandom" class where we have access to the current seed
        // and the serialize the seed as well. In our context, it doesn't matter much as the rand is not used once
        // this commulate is transferred (the rand is no used in the reduce phase)
        this.rand = new Random();
    }

    /**
     * Instantiate a new FrugalProvider
     * <p>
     * This class implements the Frugal-2U algorithm for streaming quantiles.  See
     * http://dx.doi.org/10.1007/978-3-642-40273-9_7 for original paper, and
     * http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/
     * for "layman" explanation.
     * <p>
     * Frugal-2U maintains a probabilistic estimate of the requested percentile, using
     * minimal memory to do so.  Error can grow as high as 50% under certain circumstances,
     * particularly during cold-start and when the stream drifts suddenly
     *
     * @param percents how many intervals to calculate quantiles for
     */
    public Frugal(double[] percents) {
        super(percents);
        this.steps = new int[percents.length];
        this.signs = new OpenBitSet(percents.length);
        this.signs.set(0, percents.length);
        Arrays.fill(this.steps, 1);
        this.rand = new Random();
    }

    @Override
    protected byte id() {
        return ID;
    }

    /**
     * Offer a new value to the streaming percentile algo.  May modify the current
     * estimate
     *
     * @param value Value to stream
     */
    public void offer(double value) {

        // Set estimate to first value in stream...helps to avoid fully cold starts
        if (estimates == null) {
            estimates = new double[percents.length];
            Arrays.fill(this.estimates, value);
            return;
        }

        for (int i = 0 ; i < percents.length; ++i) {
            offerTo(i, value);
        }
    }

    private void offerTo(int index, double value) {
        
        double percent = this.percents[index];

        /**
         * Movements in the same direction are rewarded with a boost to step, and
         * a big change to estimate. Movement in opposite direction gets negative
         * step boost but still a small boost to estimate
         *
         * 100% percentile doesn't need fancy algo, just save largest
         */
        if (percent == 100 && value > estimates[index]) {
            estimates[index] = value;
            return;
        }

        final double randomValue = this.rand.nextDouble() * 100.0d;

        if (value > estimates[index] && randomValue > (100.0d - percent)) {
            steps[index] += signs.get(index) ? 1 : -1;

            if (steps[index] > 0) {
                estimates[index] += steps[index];
            } else {
                ++estimates[index];
            }

            signs.set(index);

            //If we overshot, reduce step and reset estimate
            if (estimates[index] > value) {
                steps[index] += (value - estimates[index]);
                estimates[index] = value;
            }

        } else if (value < estimates[index] && randomValue < (100.0d - percent)) {
            steps[index] += signs.get(index) ? -1 : 1;

            if (steps[index] > 0) {
                estimates[index] -= steps[index];
            } else {
                --estimates[index];
            }

            signs.clear(index);

            //If we overshot, reduce step and reset estimate
            if (estimates[index] < value) {
                steps[index] += (estimates[index] - value);
                estimates[index] = value;
            }
        }

        // Smooth out oscillations
        if ((estimates[index] - value) * (signs.get(index) ? 1 : -1)  < 0 && steps[index] > 1) {
            steps[index] = 1;
        }

        // Prevent step from growing more negative than necessary
        if (steps[index] <= -Integer.MAX_VALUE + 1000) {
            steps[index] = -Integer.MAX_VALUE + 1000;
        }
    }

    public double estimate(int index) {
        return estimates != null ? estimates[index] : Double.NaN;
    }

    @Override
    public Merger merger(int expectedMerges) {
        return new Merger(expectedMerges);
    }

    public static Frugal readNewFrom(StreamInput in) throws IOException {
        Frugal frugal = new Frugal();
        frugal.readFrom(in);
        return frugal;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.percents = new double[in.readInt()];
        this.estimates = in.readBoolean() ? new double[this.percents.length] : null;
        this.steps = new int[this.percents.length];

        for (int i = 0 ; i < percents.length; ++i) {
            percents[i] = in.readDouble();
            steps[i] = in.readInt();
            if (estimates != null) {
                estimates[i] = in.readDouble();
            }
        }

        long[] bits = new long[in.readInt()];
        for (int i = 0; i < bits.length; ++i) {
            bits[i] = in.readLong();
        }
        signs = new OpenBitSet(bits, in.readInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(percents.length);
        out.writeBoolean(estimates != null);
        for (int i = 0 ; i < percents.length; ++i) {
            out.writeDouble(percents[i]);
            out.writeInt(steps[i]);
            if (estimates != null) {
                out.writeDouble(estimates[i]);
            }
        }
        long[] bits = signs.getBits();
        out.writeInt(bits.length);
        for (int i = 0; i < bits.length; ++i) {
            out.writeLong(bits[i]);
        }
        out.writeInt(signs.getNumWords());
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + percents.length * 8
                + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (estimates != null ? estimates.length * 8 : 0)
                + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + steps.length * 4
                + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + signs.getBits().length * 8 + 8 /* numBits */ + 4 /* wlen */ // signs bitset
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2 + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER * 2 + 8; // Random
    }

    /**
     * Responsible for merging multiple frugal estimators. Merging is accomplished by taking the median for
     * each percentile.  More accurate than simply averaging, though probably slower.
     */
    private class Merger implements InternalPercentiles.Estimator.Merger<Frugal> {

        private final int expectedMerges;
        private double[] merging;
        private int currentMerge;

        private Merger(int expectedMerges) {
            this.expectedMerges = expectedMerges;
        }

        @Override
        public void add(Frugal frugal) {

            if (frugal.estimates == null) {
                return;
            }

            if (merging == null) {
                merging = new double[expectedMerges * percents.length];
            }

            for (int i = 0; i < percents.length; ++i) {
                merging[(expectedMerges* i) + currentMerge] = frugal.estimate(i);
            }
            currentMerge++;
        }

        @Override
        public Frugal merge() {
            if (merging != null) {
                for (int i = 0; i < percents.length; ++i) {
                    Frugal.this.estimates[i] = QuickSelect.quickSelect(merging, i * expectedMerges, (i * expectedMerges) + expectedMerges - 1, expectedMerges / 2);
                }
            }
            return Frugal.this;
        }

    }

    public static class Factory implements InternalPercentiles.Estimator.Factory<Frugal> {

        public Frugal create(double[] percents) {
            return new Frugal(percents);
        }

    }
}