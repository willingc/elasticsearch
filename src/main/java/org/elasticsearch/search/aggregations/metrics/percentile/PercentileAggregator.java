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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

import java.io.IOException;

/**
 *
 */
public class PercentileAggregator extends Aggregator {

    private final NumericValuesSource valuesSource;
    private DoubleValues values;

    private PercentilesEstimator estimator;
    private final boolean keyed;


    public PercentileAggregator(String name, long estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context,
                                Aggregator parent, PercentilesEstimator estimator, boolean keyed) {

        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.estimator = estimator;
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.doubleValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        final int valueCount = values.setDocument(doc);
        for (int i = 0; i < valueCount; i++) {
            estimator.offer(values.nextValue(), owningBucketOrdinal);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        return new InternalPercentiles(name, estimator.flyweight(owningBucketOrdinal), keyed);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentiles(name, estimator.emptyFlyweight(), keyed);
    }


    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        private final PercentilesEstimator.Factory estimatorFactory;
        private final double[] percents;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig,
                       double[] percents, PercentilesEstimator.Factory estimatorFactory, boolean keyed) {
            super(name, InternalPercentiles.TYPE.name(), valuesSourceConfig);
            this.estimatorFactory = estimatorFactory;
            this.percents = percents;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileAggregator(name, 0, null, aggregationContext, parent, estimatorFactory.create(percents, 0), keyed);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            PercentilesEstimator estimator = estimatorFactory.create(percents, expectedBucketsCount);
            return new PercentileAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, estimator, keyed);
        }
    }

}
