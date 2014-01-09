package org.elasticsearch.search.aggregations.metrics.percentile;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class PercentileBuilder extends ValuesSourceMetricsAggregationBuilder<PercentileBuilder> {

    private double[] percentiles;
    private String executionHint;

    public PercentileBuilder(String name) {
        super(name, InternalPercentile.TYPE.name());
    }

    public PercentileBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    public PercentileBuilder percentiles(double... percentiles) {
        this.percentiles = percentiles;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);

        if (percentiles != null) {
            builder.field("percentiles", percentiles);
        }

        if (executionHint != null) {
            builder.field("execution_hint", executionHint);
        }
    }
}
