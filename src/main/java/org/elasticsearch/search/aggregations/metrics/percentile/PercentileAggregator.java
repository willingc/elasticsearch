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

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
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

    private ObjectArray<InternalPercentiles.Estimator> estimators;
    private final InternalPercentiles.Estimator.Factory percentileFactory;
    private final double[] percents;
    private final boolean keyed;


    public PercentileAggregator(String name, long estimatedBucketsCount, NumericValuesSource valuesSource, AggregationContext context,
                                Aggregator parent, InternalPercentiles.Estimator.Factory percentileFactory, double[] percents, boolean keyed) {

        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.percentileFactory = percentileFactory;
        this.percents = percents;
        this.keyed = keyed;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            estimators = BigArrays.newObjectArray(initialSize);
            for (long i = 0; i < initialSize; ++i) {
                estimators.set(i, this.percentileFactory.create(percents));
            }
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert valuesSource != null : "if value source is null, collect should never be called";

        DoubleValues values = valuesSource.doubleValues();
        if (values == null) {
            return;
        }

        if (owningBucketOrdinal >= estimators.size()) {
            long from = estimators.size();
            estimators = BigArrays.grow(estimators, owningBucketOrdinal + 1);
            for (long i = from; i < estimators.size(); ++i) {
                estimators.set(i, this.percentileFactory.create(percents));
            }
        }

        final int valueCount = values.setDocument(doc);
        InternalPercentiles.Estimator percentile = estimators.get(owningBucketOrdinal);
        for (int i = 0; i < valueCount; i++) {
            percentile.offer(values.nextValue());
        }

        estimators.set(owningBucketOrdinal, percentile);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null || owningBucketOrdinal >= estimators.size()) {
            return buildEmptyAggregation();
        }
        return new InternalPercentiles(name, estimators.get(owningBucketOrdinal), keyed);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentiles(name, percentileFactory.create(percents), keyed);
    }


    public static class Factory extends ValueSourceAggregatorFactory.LeafOnly<NumericValuesSource> {

        private final InternalPercentiles.Estimator.Factory estimatorFactory;
        private final double[] percents;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<NumericValuesSource> valuesSourceConfig,
                       InternalPercentiles.Estimator.Factory estimatorFactory, double[] percents, boolean keyed) {
            super(name, InternalPercentiles.TYPE.name(), valuesSourceConfig);
            this.estimatorFactory = estimatorFactory;
            this.percents = percents;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileAggregator(name, 0, null, aggregationContext, parent, estimatorFactory, percents, keyed);
        }

        @Override
        protected Aggregator create(NumericValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, estimatorFactory, percents, keyed);
        }
    }

}
