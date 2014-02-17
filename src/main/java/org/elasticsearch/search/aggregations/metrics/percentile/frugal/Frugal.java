package org.elasticsearch.search.aggregations.metrics.percentile.frugal;

import com.carrotsearch.hppc.DoubleArrayList;
import jsr166y.ThreadLocalRandom;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.search.aggregations.metrics.percentile.PercentilesEstimator;

import java.io.IOException;
import java.util.Random;

public class Frugal extends PercentilesEstimator {

    public final static byte ID = 0;

    private final Random rand;

    private DoubleArray mins;
    private DoubleArray maxes;
    private DoubleArray[] estimates;  // Current estimate of percentile
    private IntArray[] steps;        // Current step value for frugal-2u
    private OpenBitSet[] signs;   // Direction of last movement
    private OpenBitSet offered;

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
    public Frugal(final double[] percents, long estimatedBucketCount) {
        super(percents);
        mins = BigArrays.newDoubleArray(estimatedBucketCount);
        mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        maxes = BigArrays.newDoubleArray(estimatedBucketCount);
        maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        estimates = new DoubleArray[percents.length];
        steps = new IntArray[percents.length];
        signs = new OpenBitSet[percents.length];
        for (int i = 0; i < percents.length; i++) {
            estimates[i] = BigArrays.newDoubleArray(estimatedBucketCount);
            steps[i] = BigArrays.newIntArray(estimatedBucketCount);
            steps[i].fill(0, steps[i].size(), 1);
            signs[i] = new OpenBitSet(estimatedBucketCount);
            signs[i].set(0, signs[i].length());
        }
        offered = new OpenBitSet(estimatedBucketCount);
        this.rand = ThreadLocalRandom.current();
    }

    @Override
    public void offer(double value, long bucketOrd) {
        if (bucketOrd >= mins.size()) {
            mins = BigArrays.grow(mins, bucketOrd + 1);
            maxes = BigArrays.resize(maxes, mins.size());
            for (int i = 0; i < percents.length; ++i) {
                estimates[i] = BigArrays.resize(estimates[i], mins.size());
                final long previousSize = steps[i].size();
                steps[i] = BigArrays.resize(steps[i], mins.size());
                steps[i].fill(previousSize, mins.size(), 1);
            }
        }

        if (!offered.get(bucketOrd)) {
            offered.set(bucketOrd);
            for (int i = 0; i < estimates.length; i++) {
                BigArrays.grow(estimates[i], bucketOrd + 1);
                estimates[i].set(bucketOrd, value);
            }
            mins.set(bucketOrd, value);
            maxes.set(bucketOrd, value);
            return;
        }

        mins.set(bucketOrd, Math.min(value, mins.get(bucketOrd)));
        maxes.set(bucketOrd, Math.max(value, maxes.get(bucketOrd)));

        final double randomValue = rand.nextDouble() * 100;
        for (int i = 0 ; i < percents.length; ++i) {
            offerTo(bucketOrd, i, value, randomValue);
        }
    }

    private void offerTo(long bucketOrd, int index, double value, double randomValue) {
        double percent = percents[index];

        if (percent == 0 || percent == 100) {
            // we calculate those separately
            return;
        }

        /**
         * Movements in the same direction are rewarded with a boost to step, and
         * a big change to estimate. Movement in opposite direction gets negative
         * step boost but still a small boost to estimate
         */

        if (value > estimates[index].get(bucketOrd) && randomValue > (100.0d - percent)) {
            steps[index].increment(bucketOrd, signs[index].get(bucketOrd) ? -1 : 1);

            if (steps[index].get(bucketOrd) > 0) {
                estimates[index].increment(bucketOrd, steps[index].get(bucketOrd));
            } else {
                estimates[index].increment(bucketOrd, 1);
            }

            signs[index].clear(bucketOrd);

            //If we overshot, reduce step and reset estimate
            double estimate = estimates[index].get(bucketOrd);
            if (estimate > value) {
                steps[index].set(bucketOrd, (int) (value - estimate));
                estimates[index].set(bucketOrd, value);
            }

        } else if (value < estimates[index].get(bucketOrd) && randomValue < (100.0d - percent)) {
            steps[index].set(bucketOrd, signs[index].get(bucketOrd) ? 1 : -1);

            if (steps[index].get(bucketOrd) > 0) {
                estimates[index].increment(bucketOrd, -steps[index].get(bucketOrd));
            } else {
                estimates[index].increment(bucketOrd, -1);
            }

            signs[index].set(bucketOrd);

            //If we overshot, reduce step and reset estimate
            double estimate = estimates[index].get(bucketOrd);
            if (estimate < value) {
                steps[index].set(bucketOrd, (int) (estimate - value));
                estimates[index].set(bucketOrd, value);
            }
        }

        // Smooth out oscillations
        if ((estimates[index].get(bucketOrd) - value) * (signs[index].get(bucketOrd) ? -1 : 1)  < 0 && steps[index].get(bucketOrd) > 1) {
            steps[index].set(bucketOrd, 1);
        }

        // Prevent step from growing more negative than necessary
        if (steps[index].get(bucketOrd) <= -Integer.MAX_VALUE + 1000) {
            steps[index].set(bucketOrd, -Integer.MAX_VALUE + 1000);
        }
    }

    @Override
    public Flyweight flyweight(long bucketOrd) {
        double[] bucketEstimates = new double[percents.length];
        for (int i = 0; i < estimates.length ; i++) {
            bucketEstimates[i] = estimates[i].get(bucketOrd);
        }
        return new Flyweight(percents, bucketEstimates, mins.get(bucketOrd), maxes.get(bucketOrd));
    }

    @Override
    public Flyweight emptyFlyweight() {
        return new Flyweight(percents, null, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    public static class Flyweight extends Result<Frugal, Flyweight> {

        private double min;
        private double max;
        private double[] estimates;

        Flyweight() {
        }

        Flyweight(double[] percents, double[] estimates, double min, double max) {
            super(percents);
            this.estimates = estimates;
            this.min = min;
            this.max = max;
        }

        @Override
        protected byte id() {
            return ID;
        }

        @Override
        public double estimate(int index) {
            if (estimates == null) {
                return Double.NaN;
            }
            if (percents[index] == 0) {
                return min;
            } else if (percents[index] == 100) {
                return max;
            } else {
                return Math.max(Math.min(estimates[index], max), min);
            }
        }

        @Override
        public Merger merger(int estimatedMerges) {
            return new Merger(estimatedMerges);
        }

        public static Flyweight readNewFrom(StreamInput in) throws IOException {
            Flyweight flyweight = new Flyweight();
            flyweight.readFrom(in);
            return flyweight;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.percents = new double[in.readInt()];
            this.estimates = in.readBoolean() ? new double[this.percents.length] : null;

            if (estimates != null) {
                min = in.readDouble();
                max = in.readDouble();
            }

            for (int i = 0 ; i < percents.length; ++i) {
                percents[i] = in.readDouble();
                if (estimates != null) {
                    estimates[i] = in.readDouble();
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(percents.length);
            out.writeBoolean(estimates != null);
            if (estimates != null) {
                out.writeDouble(min);
                out.writeDouble(max);
            }
            for (int i = 0 ; i < percents.length; ++i) {
                out.writeDouble(percents[i]);
                if (estimates != null) {
                    out.writeDouble(estimates[i]);
                }
            }
        }

        class Merger implements Result.Merger<Frugal, Frugal.Flyweight> {

            private final int expectedMerges;
            private DoubleArrayList merging;

            private Merger(int expectedMerges) {
                this.expectedMerges = expectedMerges;
            }

            @Override
            public void add(Flyweight flyweight) {
                if (flyweight.estimates == null) {
                    return;
                }

                min = Math.min(min, flyweight.min);
                max = Math.max(max, flyweight.max);

                if (merging == null) {
                    merging = new DoubleArrayList(expectedMerges * percents.length);
                }

                for (int i = 0; i < percents.length; ++i) {
                    merging.add(flyweight.estimate(i));
                }
            }

            @Override
            public Result<Frugal, Flyweight> merge() {
                if (merging != null) {
                    if (estimates == null) {
                        estimates = new double[percents.length];
                    }
                    CollectionUtils.sort(merging);
                    final int numMerges = merging.size() / percents.length;
                    for (int i = 0; i < percents.length; ++i) {
                        estimates[i] = weightedValue(merging, numMerges * i + (percents[i] / 100 * (numMerges - 1)));
                    }
                }
                return Flyweight.this;
            }

            private double weightedValue(DoubleArrayList list, double index) {
                assert index <= list.size() - 1;
                final int intIndex = (int) index;
                final double d = index - intIndex;
                if (d == 0) {
                    return list.get(intIndex);
                }
                return (1 - d) * list.get(intIndex) + d * list.get(intIndex + 1);
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return -1; // todo implement
    }

    public static class Factory implements PercentilesEstimator.Factory<Frugal> {

        @Override
        public Frugal create(double[] percents, long estimatedBucketCount) {
            return new Frugal(percents, estimatedBucketCount);
        }
    }

}