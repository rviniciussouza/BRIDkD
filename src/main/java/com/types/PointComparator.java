package com.types;

import java.util.Comparator;

import com.metrics.Metric;

public class PointComparator implements Comparator<Point> {
	private Point reference;
	private com.metrics.Metric metric;

	public PointComparator(Point reference, Metric metric) {
		this.metric = metric;
		this.reference = reference;
	}

	@Override
	public int compare(Point a, Point b) {
		double dist_a = metric.distance(a, reference);
		double dist_b = metric.distance(b, reference);
		return dist_a < dist_b ? -1 : dist_a == dist_b ? 0 : 1;
	}

}
