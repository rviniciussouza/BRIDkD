package com.metrics;

import com.types.Tuple;

public class Eucledian implements Metric {

	@Override
	public double distance(Tuple s, Tuple t) {
		double distanceSquare = 0;
		for (int i = 0; i < s.getAttributes().size(); i++) {
			double diff = s.getAttributes().get(i) - t.getAttributes().get(i);
			distanceSquare += diff * diff;
		}
		return Math.sqrt(distanceSquare);
	}

	@Override
	public double hyperplanDist(Tuple p0, Tuple p1, Tuple t) {
		return Math.abs(eucledianSquare(t, p0) - eucledianSquare(t, p1))
            / (2 * distance(p0, p1));
	}

	private Double eucledianSquare(Tuple s, Tuple t) {
		double distanceSquare = 0;
        for(int i = 0; i < s.getAttributes().size(); i++) {
            double diff = s.getAttributes().get(i) - t.getAttributes().get(i);
            distanceSquare += diff * diff;
        }
        return distanceSquare;
	}
}
