package com.algorithms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.metrics.Eucledian;
import com.types.Tuple;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BridTest {
    
    List<Tuple> points = Arrays.asList(
        new Tuple(Arrays.asList(7.0, 5.0)), /* 0 - B*/ 
        new Tuple(Arrays.asList(8.0, 6.0)), /* 1 - C*/
        new Tuple(Arrays.asList(5.0, 6.0)), /* 2 - D*/
        new Tuple(Arrays.asList(3.0, 4.0)), /* 3 - E*/
        new Tuple(Arrays.asList(3.0, 3.0)), /* 4 - F*/
        new Tuple(Arrays.asList(3.0, 2.0)), /* 5 - K*/
        new Tuple(Arrays.asList(2.8, 4.6)), /* 6 - G*/
        new Tuple(Arrays.asList(6.0, 6.0)), /* 7 - I*/
        new Tuple(Arrays.asList(7.0, 4.0))  /* 8 - L*/
    );
    Brid brid;

    @BeforeEach
    public void setUp() {
        brid = new Brid(points, new Eucledian());
    }

    @Test
    public void influenceLevelTest() {
        Tuple pointA = points.get(3);
        Tuple pointB = points.get(5);

        double level = brid.influenceLevel(pointA, pointB);

        assertEquals(0.5, level);
    }

    @Test
    public void influenceLevelForEqualPointsTest() {
        Tuple pointA = points.get(3);
        Tuple pointB = points.get(3);

        double level = brid.influenceLevel(pointA, pointB);

        assertEquals(Double.MAX_VALUE, level);
    }

    @Test
    public void isStrongInfluence() {
        Tuple pointA = points.get(3);
        Tuple pointB = points.get(4);
        Tuple query = new Tuple(Arrays.asList(5.0, 5.0));
        boolean strongInfluence = brid.isStrongInfluence(pointA, pointB, query);
        assertTrue(strongInfluence);
    }

    @Test
    public void isStrongInfluenceWithPointOverBoundary() {
        Tuple pointA = points.get(3);
        Tuple pointB = points.get(6);
        Tuple query = new Tuple(Arrays.asList(5.0, 5.0));
        boolean strongInfluence = brid.isStrongInfluence(pointA, pointB, query);
        assertTrue(strongInfluence);
    }

    @Test
    public void notIsStrongInfluence() {
        Tuple pointA = points.get(3);
        Tuple pointB = points.get(2);
        Tuple query = new Tuple(Arrays.asList(5.0, 5.0));
        boolean strongInfluence = brid.isStrongInfluence(pointA, pointB, query);
        assertFalse(strongInfluence);
    }

    @Test
    public void searchTest() {
        int k = 3;
        Tuple query = new Tuple(Arrays.asList(5.0, 5.0));   
        Tuple firstPoint = this.points.get(2);
        Tuple secondPoint = this.points.get(0);
        Tuple thirdPoint = this.points.get(3);

        this.brid.dataset.stream().forEach(
            t -> t.setDistance(this.brid.metric.distance(t, query))
        );
        
        Collections.sort(
            this.brid.dataset,
            (t1, t2) -> {
                return
                    t1.getDistance() < t2.getDistance()
                    ? -1 
                    : t1.getDistance() == t2.getDistance() ? 0 : 1;
            });
        this.brid.dataset.stream().forEach(
            t -> System.out.println(t.getDistance())
        );
        System.out.println(points.get(2).getAttributes());
        List<Tuple> result = brid.search(query, k);

        assertThat(result).hasSize(3);
        assertThat(result).containsExactly(firstPoint, secondPoint, thirdPoint); 
    }

}
