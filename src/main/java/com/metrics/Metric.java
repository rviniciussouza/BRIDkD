package com.metrics;

import com.types.Point;

public abstract class Metric {
	
	public Integer numberOfCalculations = 0;

	/**
	 * Calcula a distância entre o elemento s e o element t
	 * @param s
	 * @param t
	 * @return
	 */
	public abstract double distance(Point s, Point t);

	/**
	 * Calcula a distância entre o element "t" e o hiperplano generalizado formado entre p0 e p1
	 * @param p0
	 * @param p1
	 * @param t
	 * @return
	 */
	public abstract double distHyperplane(Point p0, Point p1, Point t);
}
