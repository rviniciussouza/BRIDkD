package com.mapreduce.mapper;

import java.util.ArrayList;
import java.util.Arrays;

import com.metrics.Eucledian;
import com.types.PartitionDistancePair;
import com.types.Point;
import com.types.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class PivotBasedMapperTest {
    
    private PivotBasedMapper mapper;

    @Before
    public void setUp() {
        mapper = new PivotBasedMapper();
        mapper.metric = new Eucledian();
        mapper.pivots = new ArrayList<Point>(
            Arrays.asList(
                new Point(Arrays.asList(2.0, 2.0)),
                new Point(Arrays.asList(5.0, 5.0))
            )
        );
    }

    @Test 
    public void getIndexClosestPivot() {
        Tuple p = new Tuple(Arrays.asList(3.0, 3.0));
        int partitionId = mapper.getIndexClosestPivot(p);
        Assertions.assertEquals(0, partitionId);
    }

    @Test
    public void getIndexFirstClosestPivot() {
        Tuple p = new Tuple(Arrays.asList(3.5, 3.5));
        int partitionId = mapper.getIndexClosestPivot(p);
        Assertions.assertEquals(0, partitionId);
    }

    @Test
    public void generateOutputKeyTest() {
        /* scenario */
        int partition = 0;
        mapper.query = new Point(Arrays.asList(0.0, 2.0));
        Point point = new Point(Arrays.asList(0.0, 0.0));
        /* run */
        PartitionDistancePair pair = mapper.generateOutputKey(partition, point);
        /* asserts */
        Assertions.assertEquals(0, pair.getPartition().get());
        Assertions.assertEquals(2.0, pair.getDistance().get());
    }

    @Test
    public void processAsCorePointTest() {
        /* scenario */
        Point point = new Point(Arrays.asList(0.0, 0.0));
        mapper.query = new Point(Arrays.asList(0.0, 2.0));
        /* run */
        PartitionDistancePair pair = mapper.processAsCorePoint(point);
        /* asserts */
        Assertions.assertEquals(0, pair.getPartition().get());
        Assertions.assertEquals(2.0, pair.getDistance().get());
    }
}
