package org.elasticsearch.search.aggregations.metrics.percentile;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class PercentileBuilder extends ValuesSourceMetricsAggregationBuilder<PercentileBuilder> {

    private double[] percentiles;
    private Percentiles.ExecutionHint executionHint;

    public PercentileBuilder(String name) {
        super(name, InternalPercentiles.TYPE.name());
    }

    public PercentileBuilder percentiles(double... percentiles) {
        for (int i = 0; i < percentiles.length; i++) {
            if (percentiles[i] < 0 || percentiles[i] > 100) {
                throw new IllegalArgumentException("the percents in the percentiles aggregation [" +
                        name + "] must be in the [0, 100] range");
            }
        }
        this.percentiles = percentiles;
        return this;
    }

    public PercentileBuilder executionHint(Percentiles.ExecutionHint executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        super.internalXContent(builder, params);

        if (percentiles != null) {
            builder.field("percents", percentiles);
        }

        if (executionHint != null) {
            builder.field("execution_hint", executionHint.type());
            executionHint.paramsToXContent(builder);
        }
    }
}
