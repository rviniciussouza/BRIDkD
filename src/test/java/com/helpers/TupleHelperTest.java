package com.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.metrics.Eucledian;
import com.types.Tuple;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TupleHelperTest {

    @Test
    public void middleDistanceTest() {
        Eucledian metric = Mockito.mock(Eucledian.class);
        List<Tuple> points = Arrays.asList(
                new Tuple(Arrays.asList(0.0, 0.0)),
                new Tuple(Arrays.asList(5.0, 5.0)),
                new Tuple(Arrays.asList(5.0, 0.0)));
        when(metric.distance(
                Mockito.any(Tuple.class),
                Mockito.any(Tuple.class)))
            .thenReturn(5.0);
        double middle = TupleHelper.middleDistance(points, metric);
        assertEquals(5.0, middle);
    }

    @Test
    public void middleDistanceEmptyPointsTest() {
        Eucledian metric = Mockito.mock(Eucledian.class);
        double middle = TupleHelper.middleDistance(new ArrayList<>(), metric);
        assertEquals(0, middle);
    }
}
