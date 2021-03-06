package com.helpers;

import java.util.List;

import com.metrics.Metric;
import com.types.Point;

/**
 * Funções auxiliares para a classe Point.
 */
public class PointHelper {

    /**
     * Determina a distância média entre os pontos
     */
    public static double middleDistance(List<Point> points, Metric metric) {
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
}
