package com.algorithms;

import java.util.ArrayList;
import java.util.List;

import com.metrics.Metric;
import com.types.Tuple;

public class Brid<T extends Tuple> {
    Metric<T> metric;
    List<T> dataset;

    public Brid(List<T> dataset, Metric<T> metric) {
        this.dataset = dataset;
        this.metric = metric;
    }

    public Brid(Metric<T> metric) {
        this.metric = metric;
    }

    public List<T> search(T sq, int k) {
        List<T> result = new ArrayList<>();

        int pos = 0;
        while(result.size() < k && pos < dataset.size()) {
            T s = dataset.get(pos++);
            boolean dominant = true;
            for(T resultElement : result) {
                if(this.strongInfluenceSet(resultElement, sq).contains(s)) {
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

    public double influenceLevel(T s, T t) {
        double dist = metric.solve(s, t);
        if(dist == 0) return 1;
        return (1/dist);
    }

    public List<T> strongInfluenceSet(T s, T sq) {
        List<T> set = new ArrayList<>();
        for(T element : this.dataset) {
            if(this.isStrongInfluence(s, element, sq)) {
                set.add(element);
            }
        }
        return set;
    }

    public Boolean isStrongInfluence(T s, T t, T sq) {
        double influence_s_to_sq = this.influenceLevel(s, sq);
        double influence_s_to_t = this.influenceLevel(s, t);
        double influence_sq_to_t = this.influenceLevel(sq, t);

        return (
            influence_s_to_t >= influence_s_to_sq
            && influence_s_to_t >= influence_sq_to_t
        );
    }


}
