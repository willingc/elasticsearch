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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.percentile.Percentiles;
import org.junit.Ignore;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class PercentilesTests extends AbstractNumericTests {

    private Percentiles.ExecutionHint randomHint() {
        switch (randomInt(2)) {
            case 0:
                return Percentiles.ExecutionHint.frugal();
            case 1:
                return Percentiles.ExecutionHint.qDigest();
            default:
                return Percentiles.ExecutionHint.tDigest();
        }
    }

    @Test
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1l).emptyBuckets(true)
                        .subAggregation(percentiles("percentiles")
                                .percentiles(10, 15)
                                .executionHint(randomHint())))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getByKey(1l);
        assertThat(bucket, notNullValue());

        Percentiles percentiles = bucket.getAggregations().get("percentiles");
        assertThat(percentiles, notNullValue());
        assertThat(percentiles.getName(), equalTo("percentiles"));
        assertThat(percentiles.percentile(10), equalTo(Double.NaN));
        assertThat(percentiles.percentile(15), equalTo(Double.NaN));
    }

    @Test
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(percentiles("percentiles")
                        .field("value")
                        .percentiles(10, 15)
                        .executionHint(randomHint()))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0l));

        Percentiles percentiles = searchResponse.getAggregations().get("percentiles");
        assertThat(percentiles, notNullValue());
        assertThat(percentiles.getName(), equalTo("percentiles"));
        assertThat(percentiles.percentile(10), equalTo(Double.NaN));
        assertThat(percentiles.percentile(15), equalTo(Double.NaN));
    }

    @Test @Ignore
    public void testSingleValuedField() throws Exception {
    }

    @Test @Ignore
    public void testSingleValuedField_PartiallyUnmapped() throws Exception {
    }

    @Test @Ignore
    public void testSingleValuedField_WithValueScript() throws Exception {
    }

    @Test @Ignore
    public void testSingleValuedField_WithValueScript_WithParams() throws Exception {
    }

    @Test @Ignore
    public void testMultiValuedField() throws Exception {
    }

    @Test @Ignore
    public void testMultiValuedField_WithValueScript() throws Exception {
    }

    @Test @Ignore
    public void testMultiValuedField_WithValueScript_Reverse() throws Exception {
    }

    @Test @Ignore
    public void testMultiValuedField_WithValueScript_WithParams() throws Exception {
    }

    @Test @Ignore
    public void testScript_SingleValued() throws Exception {
    }

    @Test @Ignore
    public void testScript_SingleValued_WithParams() throws Exception {
    }

    @Test @Ignore
    public void testScript_ExplicitSingleValued_WithParams() throws Exception {
    }

    @Test @Ignore
    public void testScript_MultiValued() throws Exception {
    }

    @Test @Ignore
    public void testScript_ExplicitMultiValued() throws Exception {
    }

    @Test @Ignore
    public void testScript_MultiValued_WithParams() throws Exception {
    }

}