package com.metrics;

import com.types.Point;

public class Eucledian extends Metric {

	@Override
	public double distance(Point s, Point t) {
		double distanceSquare = 0;
		for (int i = 0; i < s.getAttributes().size(); i++) {
			double diff = s.getAttributes().get(i) - t.getAttributes().get(i);
			distanceSquare += diff * diff;
		}
		this.numberOfCalculations++;
		return Math.sqrt(distanceSquare);
	}

	@Override
	public double distHyperplane(Point p0, Point p1, Point t) {
		return Math.abs(eucledianSquare(t, p0) - eucledianSquare(t, p1))
            / (2 * distance(p0, p1));
	}

	public double eucledianSquare(Point s, Point t) {
		double distanceSquare = 0;
        for(int i = 0; i < s.getAttributes().size(); i++) {
            double diff = s.getAttributes().get(i) - t.getAttributes().get(i);
            distanceSquare += diff * diff;
        }
        return distanceSquare;
	}
}
