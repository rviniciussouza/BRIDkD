package com.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import com.types.Point;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EucledianTest {
    
    Eucledian eucledian;

    @BeforeEach
    public void setUp() {
        eucledian = new Eucledian();
    }
    
    @Test
    public void distanceTest() {
        Point pointA = new Point(Arrays.asList(0.0, 5.0));
        Point pointB = new Point(Arrays.asList(4.0, 8.0));
        double dist = eucledian.distance(pointA, pointB);
        assertEquals(5.0, dist);
    }

    @Test
    public void eucledianSquareTest() {
        Point pointA = new Point(Arrays.asList(0.0, 5.0));
        Point pointB = new Point(Arrays.asList(4.0, 8.0));
        double dist = eucledian.eucledianSquare(pointA, pointB);
        assertEquals(25.0, dist);
    }

    @Test
    public void distHyperplanTest() {
        /* cenario */
        Point pointA = new Point(Arrays.asList(0.0, 0.0));
        Point pointB = new Point(Arrays.asList(10.0, 0.0));
        Point reference = new Point(Arrays.asList(2.0, 0.0));
        /* execucao */
        double dist = eucledian.distHyperplane(pointA, pointB, reference);
        /* verificações */
        assertEquals(3.0, dist);
    }

    @Test
    public void pointOnTheHyperplane() {
        /* cenario */
        Point pointA = new Point(Arrays.asList(0.0, 0.0));
        Point pointB = new Point(Arrays.asList(10.0, 0.0));
        Point reference = new Point(Arrays.asList(5.0, 5.0));
        /* execucao */
        double dist = eucledian.distHyperplane(pointA, pointB, reference);
        /* verificações */
        assertEquals(0.0, dist);
    }

}
