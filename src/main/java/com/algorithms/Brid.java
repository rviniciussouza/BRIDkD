package com.algorithms;

import java.util.ArrayList;
import java.util.List;
import com.metrics.Metric;
import com.types.Tuple;

/**
 * Implementação do algoritmo BRIDk.
 * Dado um conjunto de dados de entrada e um objeto de consulta sq, esse algoritmo
 * retorna o k vizinhos diversificados do objeto de consulta. 
 */
public class Brid {
	Metric metric;
	List<Tuple> dataset;

	public Brid(List<Tuple> dataset, Metric metric) {
		this.dataset = dataset;
		this.metric = metric;
	}

	public Brid(Metric metric) {
		this.metric = metric;
	}

	public List<Tuple> search(Tuple sq, int k) {
		List<Tuple> result = new ArrayList<>();
        int pos = 0;
        while(result.size() < k && pos < dataset.size()) {
            Tuple s = dataset.get(pos++);
            boolean dominant = true;
            for(Tuple resultElement : result) {
                if(this.isStrongInfluence(resultElement, s, sq)) {
                    dominant = false;
                    break;
                }
            }
            if(dominant) {
                result.add(s);
            }
        }
        return result;
	}

	public double influenceLevel(Tuple s, Tuple t) {
		double dist = metric.distance(s, t);
		if (dist == 0) return Double.MAX_VALUE;
		return (1 / dist);
	}

	public List<Tuple> strongInfluenceSet(Tuple s, Tuple sq) {
		List<Tuple> set = new ArrayList<>();
        for(Tuple element : this.dataset) {
            if(this.isStrongInfluence(s, element, sq)) {
                set.add(element);
            }
        }
        return set;
	}

	public Boolean isStrongInfluence(Tuple s, Tuple t, Tuple sq) {
		double influence_s_to_sq = this.influenceLevel(s, sq);
        double influence_s_to_t = this.influenceLevel(s, t);
        double influence_sq_to_t = this.influenceLevel(sq, t);

        return (
            influence_s_to_t >= influence_s_to_sq
            && influence_s_to_t >= influence_sq_to_t
        );
	}

}
