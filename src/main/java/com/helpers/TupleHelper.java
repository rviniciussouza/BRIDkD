package com.helpers;

import java.util.List;

import com.metrics.Metric;
import com.types.Tuple;

public class TupleHelper {

    public static double middleDistance(List<Tuple> points, Metric metric) {
        double sumDistance = .0;
        for(int i = 0; i < points.size(); i++) {
            for(int j = i+1; j < points.size(); j++) {
                sumDistance += metric.distance(points.get(i), points.get(j));
            }
        }
        if(points.size() > 0) {
            return sumDistance / (double)points.size();
        }
        return 0;
    }

    public static double minimumDistance(int reference, List<Tuple> points, Metric metric) throws IllegalArgumentException {
        Double shortest_distance = Double.MAX_VALUE;
        try {
            for(int i = 0; i < points.size(); i++) {
                if(i != reference) {
                    Double cur_distance = metric.distance(points.get(reference), points.get(i));
                    shortest_distance = Double.min(cur_distance, shortest_distance);
                }
            }
            return shortest_distance;
        }
        catch(ArrayIndexOutOfBoundsException exception) {
            throw new IllegalArgumentException("A referência passada como argumento é invalida");
        }
    }
}
