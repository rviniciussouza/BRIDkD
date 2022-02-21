package com.algorithms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.metrics.Eucledian;
import com.types.Point;
import com.types.PointComparator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BridTest {
    
    /**
     * Representação gráfica do exemplo utilizados no teste:
     *  https://ibb.co/3MFNBDM
     */
    List<Point> points = Arrays.asList(
        new Point(Arrays.asList(7.0, 5.0)), /* 0 - B*/ 
        new Point(Arrays.asList(8.0, 6.0)), /* 1 - C*/
        new Point(Arrays.asList(5.0, 6.0)), /* 2 - D*/
        new Point(Arrays.asList(3.0, 4.0)), /* 3 - E*/
        new Point(Arrays.asList(3.0, 3.0)), /* 4 - F*/
        new Point(Arrays.asList(3.0, 2.0)), /* 5 - K*/
        new Point(Arrays.asList(2.8, 4.6)), /* 6 - G*/
        new Point(Arrays.asList(6.0, 6.0)), /* 7 - I*/
        new Point(Arrays.asList(7.0, 4.0))  /* 8 - L*/
    );
    Brid<Point> brid;

    @BeforeEach
    public void setUp() {
        brid = new Brid<Point>(points, new Eucledian());
    }

    @Test
    public void influenceLevelTest() {
        Point pointA = points.get(3);
        Point pointB = points.get(5);

        double level = brid.influenceLevel(pointA, pointB);

        assertEquals(0.5, level);
    }

    @Test
    public void influenceLevelForEqualPointsTest() {
        Point pointA = points.get(3);
        Point pointB = points.get(3);

        double level = brid.influenceLevel(pointA, pointB);

        assertEquals(Double.MAX_VALUE, level);
    }

    @Test
    public void isStrongInfluence() {
        Point pointA = points.get(3);
        Point pointB = points.get(4);
        Point query = new Point(Arrays.asList(5.0, 5.0));
        boolean strongInfluence = brid.isStrongInfluence(pointA, pointB, query);
        assertTrue(strongInfluence);
    }

    @Test
    public void isStrongInfluenceWithPointOverBoundary() {
        Point pointA = points.get(3);
        Point pointB = points.get(6);
        Point query = new Point(Arrays.asList(5.0, 5.0));
        boolean strongInfluence = brid.isStrongInfluence(pointA, pointB, query);
        assertTrue(strongInfluence);
    }

    @Test
    public void notIsStrongInfluence() {
        Point pointA = points.get(3);
        Point pointB = points.get(2);
        Point query = new Point(Arrays.asList(5.0, 5.0));
        boolean strongInfluence = brid.isStrongInfluence(pointA, pointB, query);
        assertFalse(strongInfluence);
    }

    @Test
    public void searchTest() {
        /* scenario */
        int k = 3;
        Point query = new Point(Arrays.asList(5.0, 5.0));   
        Point firstPoint = this.points.get(2);
        Point secondPoint = this.points.get(0);
        Point thirdPoint = this.points.get(3);
        Collections.sort(points, new PointComparator(query, new Eucledian()));
        /* run */
        List<Point> result = brid.search(query, k);
        /* assets */
        assertThat(result).hasSize(3);
        assertThat(result).containsExactly(firstPoint, secondPoint, thirdPoint); 
    }
}
