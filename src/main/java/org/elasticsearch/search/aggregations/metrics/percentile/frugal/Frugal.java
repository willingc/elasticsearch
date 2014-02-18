package org.elasticsearch.search.aggregations.metrics.percentile.frugal;

import com.carrotsearch.hppc.DoubleArrayList;
import jsr166y.ThreadLocalRandom;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.search.aggregations.metrics.percentile.PercentilesEstimator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Random;

public class Frugal extends PercentilesEstimator {

    public final static byte ID = 0;

    private final Random rand;

    private DoubleArray mins;
    private DoubleArray maxes;
    private DoubleArray[] estimates;  // Current estimate of percentile
    private DoubleArray[] steps;      // Current step value for frugal-2u
    private OpenBitSet[] signs;       // Direction of last movement

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
    public Frugal(final double[] percents, long estimatedBucketCount, AggregationContext context) {
        super(percents);
        final PageCacheRecycler recycler = context.pageCacheRecycler();
        mins = BigArrays.newDoubleArray(estimatedBucketCount, recycler, false);
        mins.fill(0, mins.size(), Double.NaN);
        maxes = BigArrays.newDoubleArray(estimatedBucketCount, recycler, false);
        estimates = new DoubleArray[percents.length];
        steps = new DoubleArray[percents.length];
        signs = new OpenBitSet[percents.length];
        for (int i = 0; i < percents.length; i++) {
            if (percents[i] == 0 || percents[i] == 100) {
                continue;
            }
            estimates[i] = BigArrays.newDoubleArray(estimatedBucketCount, recycler, false);
            steps[i] = BigArrays.newDoubleArray(estimatedBucketCount, recycler, false);
            steps[i].fill(0, steps[i].size(), 1);
            signs[i] = new OpenBitSet(estimatedBucketCount);
        }
        this.rand = ThreadLocalRandom.current();
    }

    @Override
    public void offer(double value, long bucketOrd) {
        if (bucketOrd >= mins.size()) {
            final long originalSize = mins.size();
            mins = BigArrays.grow(mins, bucketOrd + 1);
            mins.fill(originalSize, mins.size(), Double.NaN);
            maxes = BigArrays.resize(maxes, mins.size());
            for (int i = 0; i < percents.length; ++i) {
                if (estimates[i] == null) {
                    continue;
                }
                estimates[i] = BigArrays.resize(estimates[i], mins.size());
                steps[i] = BigArrays.resize(steps[i], mins.size());
                steps[i].fill(originalSize, mins.size(), 1);
            }
        }

        if (Double.isNaN(mins.get(bucketOrd))) {
            for (int i = 0; i < estimates.length; i++) {
                if (estimates[i] != null) {
                    estimates[i].set(bucketOrd, value);
                }
            }
            mins.set(bucketOrd, value);
            maxes.set(bucketOrd, value);
        } else {
            mins.set(bucketOrd, Math.min(value, mins.get(bucketOrd)));
            maxes.set(bucketOrd, Math.max(value, maxes.get(bucketOrd)));

            final double randomValue = rand.nextDouble() * 100;
            for (int i = 0 ; i < percents.length; ++i) {
                offerTo(bucketOrd, i, value, randomValue);
            }
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

        double step = steps[index].get(bucketOrd);
        double estimate = estimates[index].get(bucketOrd);
        int sign = signs[index].get(bucketOrd) ? -1 : 1;
        if (value > estimate && randomValue > (100 - percent)) {
            step += sign;
            estimate += step > 0 ? step : 1;
            sign = 1;
            // If we overshot, reduce step and reset estimate
            if (estimate > value) {
                step -= (estimate - value);
                estimate = value;
            }
        } else if (value < estimate && randomValue > percent) {
            step -= sign;
            estimate -= step > 0 ? step : 1;
            sign = -1;
            // If we overshot, reduce step and reset estimate
            if (estimate < value) {
                step -= (value - estimate);
                estimate = value;
            }
        }

        // Smooth out oscillations
        if ((estimate - value) * sign < 0 && step > 1) {
            step = 1;
        }

        steps[index].set(bucketOrd, step);
        estimates[index].set(bucketOrd, estimate);
        if (sign == -1) {
            signs[index].set(bucketOrd);
        } else {
            signs[index].clear(bucketOrd);
        }
    }

    private double estimate(int index, long bucketOrd) {
        if (percents[index] == 0) {
            return mins.get(bucketOrd);
        } else if (percents[index] == 100) {
            return maxes.get(bucketOrd);
        } else {
            return estimates[index].get(bucketOrd);
        }
    }

    @Override
    public Flyweight flyweight(long bucketOrd) {
        if (bucketOrd >= mins.size() || Double.isNaN(mins.get(bucketOrd))) {
            return emptyFlyweight();
        }
        double[] bucketEstimates = new double[percents.length];
        for (int i = 0; i < estimates.length ; i++) {
            bucketEstimates[i] = estimate(i, bucketOrd);
        }
        return new Flyweight(percents, bucketEstimates, mins.get(bucketOrd), maxes.get(bucketOrd));
    }

    @Override
    public Flyweight emptyFlyweight() {
        return new Flyweight(percents, null, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    public static class Flyweight extends Result<Frugal, Flyweight> {

        private double[] estimates;

        Flyweight() {
        }

        Flyweight(double[] percents, double[] estimates, double min, double max) {
            super(percents);
            this.estimates = estimates;
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
            return estimates[index];
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
        public Frugal create(double[] percents, long estimatedBucketCount, AggregationContext context) {
            return new Frugal(percents, estimatedBucketCount, context);
        }
    }
}