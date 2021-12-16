package com.metrics;

import com.types.Tuple;

public abstract class Metric {
	
	public Integer numberOfCalculations = 0;

	/**
	 * Calcula a distância entre o elemento s e o element t
	 * @param s
	 * @param t
	 * @return
	 */
	public abstract double distance(Tuple s, Tuple t);

	/**
	 * Calcula a distância entre o element "t" e o hiperplano generalizado formado entre p0 e p1
	 * @param p0
	 * @param p1
	 * @param t
	 * @return
	 */
	public abstract double hyperplanDist(Tuple p0, Tuple p1, Tuple t);
}
