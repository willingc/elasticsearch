/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentile.frugal.Frugal;
import org.elasticsearch.search.aggregations.metrics.percentile.qdigest.QDigest;
import org.elasticsearch.search.aggregations.metrics.percentile.tdigest.TDigest;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatterStreams;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
*
*/
public class InternalPercentiles extends MetricsAggregation.MultiValue implements Percentiles {

    public final static Type TYPE = new Type("percentile");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPercentiles readResult(StreamInput in) throws IOException {
            InternalPercentiles result = new InternalPercentiles();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private Estimator estimator;
    private boolean keyed;

    InternalPercentiles() {} // for serialization

    public InternalPercentiles(String name, Estimator estimator, boolean keyed) {
        super(name);
        this.estimator = estimator;
        this.keyed = keyed;
    }

    @Override
    public double value(String name) {
        return estimator.estimate(Double.valueOf(name));
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double percentile(double percent) {
        return estimator.estimate(percent);
    }

    @Override
    public Iterator<Percentiles.Percentile> iterator() {
        return new Iter(estimator);
    }

    @Override
    public InternalPercentiles reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        InternalPercentiles first = (InternalPercentiles) aggregations.get(0);
        if (aggregations.size() == 1) {
            return first;
        }
        Estimator.Merger<Estimator> merger = first.estimator.merger(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            merger.add(((InternalPercentiles) aggregation).estimator);
        }
        first.estimator = merger.merge();
        return first;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        estimator = Estimator.Streams.read(in);
        keyed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        Estimator.Streams.write(estimator, out);
        out.writeBoolean(keyed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        double[] percents = estimator.percents();
        if (keyed) {
            builder.startObject(name);
            for(int i = 0; i < percents.length; ++i) {
                String key = String.valueOf(percents[i]);
                double value = estimator.estimate(i);
                builder.field(key, value);
                if (valueFormatter != null) {
                    builder.field(key + "_as_string", valueFormatter.format(value));
                }
            }
            builder.endObject();
        } else {
            builder.startArray(name);
            for (int i = 0; i < percents.length; i++) {
                double value = estimator.estimate(i);
                builder.startObject();
                builder.field(CommonFields.KEY, percents[i]);
                builder.field(CommonFields.VALUE, value);
                if (valueFormatter != null) {
                    builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(value));
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    public abstract static class Estimator<E extends Estimator> implements Streamable {

        protected double[] percents;

        protected Estimator() {} // for serialization

        public Estimator(double[] percents) {
            this.percents = percents;
        }

        protected abstract byte id();

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
        public abstract void offer(double value);

        public double estimate(double percent) {
            int i = Arrays.binarySearch(percents, percent);
            assert i >= 0;
            return estimate(i);
        }

        public abstract double estimate(int index);

        public abstract Merger<E> merger(int expectedMerges);

        public abstract long ramBytesUsed();

        /**
         * Responsible for merging multiple estimators into a single one.
         */
        public static interface Merger<E> {

            void add(E e);

            E merge();
        }

        public static interface Factory<E extends Estimator<E>> {

            public abstract E create(double[] percents);

        }

        static class Streams {

            static Estimator read(StreamInput in) throws IOException {
                switch (in.readByte()) {
                    case Frugal.ID: return Frugal.readNewFrom(in);
                    case QDigest.ID : return QDigest.readNewFrom(in);
                    case TDigest.ID: return TDigest.readNewFrom(in);
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unknown percentie estimator");
                }
            }

            static void write(Estimator estimator, StreamOutput out) throws IOException {
                out.writeByte(estimator.id());
                estimator.writeTo(out);
            }

        }

    }

    public static class Iter extends UnmodifiableIterator<Percentiles.Percentile> {

        private final Estimator estimator;
        private int i;

        public Iter(Estimator estimator) {
            this.estimator = estimator;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < estimator.percents.length;
        }

        @Override
        public Percentiles.Percentile next() {
            return new InnerPercentile(estimator.percents[i], estimator.estimate(i));
        }
    }

    private static class InnerPercentile implements Percentiles.Percentile {

        private final double percent;
        private final double value;

        private InnerPercentile(double percent, double value) {
            this.percent = percent;
            this.value = value;
        }

        @Override
        public double getPercent() {
            return percent;
        }

        @Override
        public double getValue() {
            return value;
        }
    }
}
