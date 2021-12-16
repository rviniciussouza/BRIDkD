package com.mapreduce.partitioner;

import com.types.PartitionDistancePair;
import com.types.TupleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionDistancePartitioner 
    extends Partitioner<PartitionDistancePair, TupleWritable>  {
    
    @Override
    public int getPartition(PartitionDistancePair pair, 
        TupleWritable tupleWritable, 
        int numberOfPartitions) {
        return Math.abs(pair.getPartition().hashCode() % numberOfPartitions);
    }
}
