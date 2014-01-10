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

import org.elasticsearch.search.aggregations.metrics.percentile.frugal.Frugal;
import org.elasticsearch.search.aggregations.metrics.percentile.qdigest.QDigest;
import org.elasticsearch.search.aggregations.metrics.percentile.tdigest.TDigest;

import java.util.Map;

/**
 *
 */
public enum InternalExecutionHint {

    FRUGAL() {
        @Override
        public InternalPercentiles.Estimator.Factory estimatorFactory(Map <String, Object> settings) {
            return new Frugal.Factory();
        }
    },

    TDIGEST() {
        @Override
        public InternalPercentiles.Estimator.Factory estimatorFactory(Map <String, Object> settings) {
            return new TDigest.Factory(settings);
        }
    },

    QDIGEST() {
        @Override
        public InternalPercentiles.Estimator.Factory estimatorFactory(Map <String, Object> settings) {
            return new QDigest.Factory(settings);
        }
    };

    public abstract InternalPercentiles.Estimator.Factory estimatorFactory(Map <String, Object> settings);

    public static InternalExecutionHint resolve(String name) {
        if (name.equals("frugal")) {
            return FRUGAL;
        }
        if (name.equals("qdigest")) {
            return QDIGEST;
        }
        if (name.equals("tdigest")) {
            return TDIGEST;
        }
        return null;
    }

}
