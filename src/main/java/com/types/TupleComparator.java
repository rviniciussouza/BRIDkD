package com.types;
import java.util.Comparator;

import com.metrics.Metric;

public class TupleComparator<T extends Tuple> implements Comparator<T> {
    private T reference;
    private com.metrics.Metric<T> metric;

    public TupleComparator(T reference, Metric<T> metric) {
        this.metric = metric;
        this.reference = reference;
    }

    @Override
    public int compare(T a, T b) {
        double dist_a = metric.solve(a, reference);
        double dist_b = metric.solve(b, reference);
        return dist_a < dist_b ? -1 : dist_a == dist_b ? 0 : 1;
    }

}
