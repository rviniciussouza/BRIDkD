package com.mapreduce.grouping;

import com.types.PartitionDistancePair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PartitionDistanceGroupingComparator
    extends WritableComparator {
    
    public PartitionDistanceGroupingComparator() {
        super(PartitionDistancePair.class, true);
    }

    @Override
    /**
     * Compare two objects
     * 
     * @param wc1 a WritableComparable object, which represents a PartitionDistancePair
     * @param wc2 a WritableComparable object, which represents a PartitionDistancePair
     * @return 0, 1, or -1 (depending on the comparison of two PartitionDistancePair objects).
     */
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        PartitionDistancePair pair = (PartitionDistancePair) wc1;
        PartitionDistancePair pair2 = (PartitionDistancePair) wc2;
        return pair.getPartition().compareTo(pair2.getPartition());
    }    
}
