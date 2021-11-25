package com.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * O PartitionDistancePair nos permite implementar uma chave 
 * composta que pode ser usado para aplicar o design pattern seconday sort
 * do MapReduce.
 * @see https://www.oreilly.com/library/view/data-algorithms/9781491906170/ch01.html
 */
public class PartitionDistancePair implements WritableComparable<PartitionDistancePair>{
    private final IntWritable partition = new IntWritable();
    private final DoubleWritable distance = new DoubleWritable();

    public PartitionDistancePair(){};

    @Override
    public void write(DataOutput out) throws IOException {
        partition.write(out);
        distance.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        partition.readFields(in);
        distance.readFields(in);
    }

    @Override
    public int compareTo(PartitionDistancePair pair) {
        int compareValue = this.partition.compareTo(pair.getPartition());
        if (compareValue == 0) {
            compareValue = distance.compareTo(pair.getDistance());
        }
        return compareValue; 		// to sort ascending 
        // return -1*compareValue;     // to sort descending 
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
           return true;
        }
        if (o == null || getClass() != o.getClass()) {
           return false;
        }

        PartitionDistancePair that = (PartitionDistancePair) o;
        if (distance != null ? !distance.equals(that.distance) : that.distance != null) {
           return false;
        }
        if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
           return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        result = 31 * result + (distance != null ? distance.hashCode() : 0);
        return result;
    }

    public IntWritable getPartition() {
        return partition;
    } 

    public void setPartition(int partIdx) {
        this.partition.set(partIdx);
    }

    public DoubleWritable getDistance() {
        return distance;
    } 

    public void setDistance(Double dist) {
        this.distance.set(dist);
    }
}
