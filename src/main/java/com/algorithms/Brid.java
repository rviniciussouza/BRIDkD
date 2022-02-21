package com.algorithms;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.metrics.Metric;
import com.types.Point;

/**
 * Implementação do algoritmo BRIDk.
 * Dado um conjunto de dados de entrada e um objeto de consulta sq, esse algoritmo
 * retorna o k vizinhos diversificados do objeto de consulta. 
 */
public class Brid<T extends Point> {
	Metric metric;
	List<T> dataset;

	public Brid(List<T> dataset, Metric metric) {
		this.dataset = dataset;
		this.metric = metric;
	}

	public Brid(Metric metric) {
		this.metric = metric;
	}

    /**
     * Search for k diversified nearest neighbors
     */
	public List<T> search(T query, int k) {
		List<T> result = new ArrayList<>();
        int pos = 0;
        while(result.size() < k && pos < dataset.size()) {
            T candidate = dataset.get(pos++);
            if(notInfluenced(candidate, query, result)) {
                result.add(candidate);
            }
        }
        return result;
	}

    /**
     * Compute the level of influence "s" exerts on "t"
     */
	public double influenceLevel(T s, T t) {
		double dist = metric.distance(s, t);
		return (dist == 0 ? Double.MAX_VALUE : (1 / dist));
	}

    /**
     * Check that the candidate object is not influenced
     * by any other object in the response set.
     */
    public boolean notInfluenced(T candidate, T query, List<T> resultSet) {
        /** Iterate over list resultSet in the reverse order */
        boolean ans = true;
        ListIterator<T> iterator = resultSet.listIterator(resultSet.size());
        while(iterator.hasPrevious()) {
            T resultElement = iterator.previous();
            if(this.isStrongInfluence(resultElement, candidate, query)) {
                ans = false;
                break;
            }
        }
        return ans;
    }

    /**
     * Computes whether "s" is a strong influence on "t" with respect to "query"
     */
	public Boolean isStrongInfluence(T s, T t, T query) {
		double influence_s_to_query = this.influenceLevel(s, query);
        double influence_s_to_t = this.influenceLevel(s, t);
        double influence_query_to_t = this.influenceLevel(query, t);

        return (
            influence_s_to_t >= influence_s_to_query
            && influence_s_to_t >= influence_query_to_t
        );
	}

}
