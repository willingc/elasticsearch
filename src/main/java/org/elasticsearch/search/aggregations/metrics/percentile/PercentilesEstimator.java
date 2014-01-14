/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.percentile;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.aggregations.metrics.percentile.frugal.Frugal;
import org.elasticsearch.search.aggregations.metrics.percentile.tdigest.TDigest;

import java.io.IOException;
import java.util.Arrays;

/**
*
*/
public abstract class PercentilesEstimator {

    protected double[] percents;

    public PercentilesEstimator(double[] percents) {
        this.percents = percents;
    }

    /**
     * @return list of percentile intervals
     */
    public double[] percents() {
        return percents;
    }

    /**
     * Offer a new value to the streaming percentile algo.  May modify the current
     * estimate
     *
     * @param value Value to stream
     */
    public abstract void offer(double value, long bucketOrd);

    public abstract Result flyweight(long bucketOrd);

    public abstract Result emptyFlyweight();

    public abstract long ramBytesUsed();

    /**
     * Responsible for merging multiple estimators into a single one.
     */
    public abstract static class Result<E extends PercentilesEstimator, F extends Result<E, F>> implements Streamable {

        protected double[] percents;

        protected Result() {} // for serialization

        protected Result(double[] percents) {
            this.percents = percents;
        }

        protected abstract byte id();

        public double estimate(double percent) {
            int i = Arrays.binarySearch(percents, percent);
            assert i >= 0;
            return estimate(i);
        }

        public abstract double estimate(int index);

        public abstract Merger<E, F> merger(int estimatedMerges);

        public static interface Merger<E extends PercentilesEstimator, F extends Result<E, F>> {

            public abstract void add(F flyweight);

            public abstract Result<E, F> merge();
        }
    }

    public static interface Factory<E extends PercentilesEstimator> {

        public abstract E create(double[] percents, long estimatedBucketCount);

    }

    static class Streams {

        static Result read(StreamInput in) throws IOException {
            switch (in.readByte()) {
                case Frugal.ID: return Frugal.Flyweight.readNewFrom(in);
                case TDigest.ID: return TDigest.Flyweight.read(in);
                default:
                    throw new ElasticsearchIllegalArgumentException("Unknown percentile estimator");
            }
        }

        static void write(Result estimator, StreamOutput out) throws IOException {
            out.writeByte(estimator.id());
            estimator.writeTo(out);
        }

    }

}
