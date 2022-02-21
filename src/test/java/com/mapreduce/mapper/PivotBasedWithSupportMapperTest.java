package com.mapreduce.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.metrics.Eucledian;
import com.types.PartitionDistancePair;
import com.types.Point;
import com.types.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class PivotBasedWithSupportMapperTest {

    private PivotBasedWithSupportMapper mapper;

    @Before
    public void setUp() {
        mapper = new PivotBasedWithSupportMapper();
        mapper.metric = new Eucledian();
        mapper.pivots = new ArrayList<Point>(
            Arrays.asList(
                new Point(Arrays.asList(0.0, 0.0)),
                new Point(Arrays.asList(5.0, 0.0)),
                new Point(Arrays.asList(10.0, 0.0)),
                new Point(Arrays.asList(0.0, 2.0))
            )
        );
    }

    @Test
    public void inTheSupportArea() {
        mapper.factor = .5;
        Tuple s = new Tuple(Arrays.asList(1.25, 0.0));
        boolean result = mapper.supportArea(s, mapper.pivots.get(0), mapper.pivots.get(1));
        Assertions.assertTrue(result);
    }

    @Test
    public void outTheSupportArea() {
        mapper.factor = .5;
        Tuple s = new Tuple(Arrays.asList(1.24, 0.0));
        boolean result = mapper.supportArea(s, mapper.pivots.get(0), mapper.pivots.get(1));
        Assertions.assertFalse(result);
    }

    @Test
    public void supportingOnlyOnePartition() {
        /* scenario */
        mapper.factor = .5;
        mapper.query = new Point(Arrays.asList(0.0, 2.0));
        Point point = new Point(Arrays.asList(1.25, .0));
        /* run */
        List<PartitionDistancePair> supported = mapper.processAsSupportPoint(point);
        /* asserts */
        assertThat(supported).hasSize(1);
    }

    @Test
    public void supportingMultiplePartition() {
        /* scenario */
        mapper.factor = .5;
        mapper.query = new Point(Arrays.asList(0.0, 2.0));
        Point point = new Point(Arrays.asList(1.25, .5));
        /* run */
        List<PartitionDistancePair> supported = mapper.processAsSupportPoint(point);
        /* asserts */
        assertThat(supported).hasSize(2);
    }

    @Test
    public void notASupportPoint() {
        /* scenario */
        mapper.factor = .5;
        Point point = new Point(Arrays.asList(1.24, .49));
        /* run */
        List<PartitionDistancePair> supported = mapper.processAsSupportPoint(point);
        /* asserts */
        assertThat(supported).hasSize(0);
    }

    @Test
    public void getThresholdDefault() {
        /* scenario */
        mapper.factor = 1.0;
        /* run */
        double threshold = mapper.getThresold(mapper.pivots.get(0), mapper.pivots.get(1));
        /* asserts */
        assertEquals(2.5, threshold);
    }

    @Test
    public void getThresholdWithHalfFactor() {
        /* scenario */
        mapper.factor = 0.5;
        /* run */
        double threshold = mapper.getThresold(mapper.pivots.get(0), mapper.pivots.get(1));
        /* asserts */
        assertEquals(1.25, threshold);
    }

    @Test
    public void getThresholdWithZeroFactor() {
        /* scenario */
        mapper.factor = .0;
        /* run */
        double threshold = mapper.getThresold(mapper.pivots.get(0), mapper.pivots.get(1));
        /* asserts */
        assertEquals(0.0, threshold);
    }
}
