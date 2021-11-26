package com.metrics;

import com.types.Tuple;

public interface Metric {
	
	/**
	 * Calcula a distância entre o elemento s e o element t
	 * @param s
	 * @param t
	 * @return
	 */
	public double distance(Tuple s, Tuple t);

	/**
	 * Calcula a distância entre o element "t" e o hiperplano generalizado formado entre p0 e p1
	 * @param p0
	 * @param p1
	 * @param t
	 * @return
	 */
	public double hyperplanDist(Tuple p0, Tuple p1, Tuple t);
}
