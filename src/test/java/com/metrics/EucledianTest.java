package com.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import com.types.Tuple;

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
        Tuple pointA = new Tuple(Arrays.asList(0.0, 5.0));
        Tuple pointB = new Tuple(Arrays.asList(4.0, 8.0));
        double dist = eucledian.distance(pointA, pointB);
        assertEquals(5.0, dist);
    }

    @Test
    public void eucledianSquareTest() {
        Tuple pointA = new Tuple(Arrays.asList(0.0, 5.0));
        Tuple pointB = new Tuple(Arrays.asList(4.0, 8.0));
        double dist = eucledian.eucledianSquare(pointA, pointB);
        assertEquals(25.0, dist);
    }

    @Test
    public void distHyperplanTest() {
        /* cenario */
        Tuple pointA = new Tuple(Arrays.asList(0.0, 0.0));
        Tuple pointB = new Tuple(Arrays.asList(10.0, 0.0));
        Tuple reference = new Tuple(Arrays.asList(2.0, 0.0));
        /* execucao */
        double dist = eucledian.hyperplanDist(pointA, pointB, reference);
        /* verificações */
        assertEquals(3.0, dist);
    }

    @Test
    public void pointOnTheHyperplane() {
        /* cenario */
        Tuple pointA = new Tuple(Arrays.asList(0.0, 0.0));
        Tuple pointB = new Tuple(Arrays.asList(10.0, 0.0));
        Tuple reference = new Tuple(Arrays.asList(5.0, 5.0));
        /* execucao */
        double dist = eucledian.hyperplanDist(pointA, pointB, reference);
        /* verificações */
        assertEquals(0.0, dist);
    }

}
