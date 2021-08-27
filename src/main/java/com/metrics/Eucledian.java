package com.metrics;
import com.types.Tuple;

public class Eucledian<T extends Tuple> implements Metric<T> {

    @Override
    public double solve(T s, T t) {
        double distanceSquare = 0;
        for(int i = 0; i < s.getAttributes().size(); i++) {
            double diff = s.getAttributes().get(i) - t.getAttributes().get(i);
            distanceSquare += diff * diff;
        }
        return Math.sqrt(distanceSquare);
    }
}
