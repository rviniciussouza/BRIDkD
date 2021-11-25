package com.types;

import java.util.Comparator;

import com.metrics.Metric;

public class TupleComparator implements Comparator<Tuple> {
	private Tuple reference;
	private com.metrics.Metric metric;

	public TupleComparator(Tuple reference, Metric metric) {
		this.metric = metric;
		this.reference = reference;
	}

	@Override
	public int compare(Tuple a, Tuple b) {
		double dist_a = metric.solve(a, reference);
		double dist_b = metric.solve(b, reference);
		return dist_a < dist_b ? -1 : dist_a == dist_b ? 0 : 1;
	}

}
