package com.metrics;

import com.types.Tuple;

public class Eucledian implements Metric {

	@Override
	public double solve(Tuple s, Tuple t) {
		double distanceSquare = 0;
		for (int i = 0; i < s.getAttributes().size(); i++) {
			double diff = s.getAttributes().get(i) - t.getAttributes().get(i);
			distanceSquare += diff * diff;
		}
		return Math.sqrt(distanceSquare);
	}
}
