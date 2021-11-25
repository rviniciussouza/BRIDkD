package com.mapreduce.partitioner;

import com.types.PartitionDistancePair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionDistancePartitioner 
    extends Partitioner<PartitionDistancePair, IntWritable>  {
    
    @Override
    public int getPartition(PartitionDistancePair pair, 
        IntWritable partitionIdx, 
        int numberOfPartitions) {
        return Math.abs(pair.getPartition().hashCode() % numberOfPartitions);
    }
}
