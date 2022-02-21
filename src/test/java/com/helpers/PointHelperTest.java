package com.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.metrics.Eucledian;
import com.types.Point;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PointHelperTest {

    @Test
    public void middleDistanceTest() {
        Eucledian metric = Mockito.mock(Eucledian.class);
        List<Point> points = Arrays.asList(
                new Point(Arrays.asList(0.0, 0.0)),
                new Point(Arrays.asList(5.0, 5.0)),
                new Point(Arrays.asList(5.0, 0.0)));
        when(metric.distance(
                Mockito.any(Point.class),
                Mockito.any(Point.class)))
            .thenReturn(5.0);
        double middle = PointHelper.middleDistance(points, metric);
        assertEquals(5.0, middle);
    }

    @Test
    public void middleDistanceEmptyPointsTest() {
        Eucledian metric = Mockito.mock(Eucledian.class);
        double middle = PointHelper.middleDistance(new ArrayList<>(), metric);
        assertEquals(0, middle);
    }
}
