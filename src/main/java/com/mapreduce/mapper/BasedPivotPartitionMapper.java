package com.mapreduce.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import com.helpers.HDFSRead;
import com.helpers.ParserTuple;
import com.types.PartitionDistancePair;
import com.types.Tuple;
import com.types.TupleWritable;


public class BasedPivotPartitionMapper extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

    List<Tuple> pivots;

    @Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.setFormatData(conf.getStrings("header_dataset"));
		ParserTuple parserQuery = new ParserTuple();
		this.query = parserQuery.parse(conf.get("query"));
		this.numberPartitions = conf.getInt("number_partitions", 2);
		this.setMetric();
        try {
            this.setPivots(conf.get("path_file_pivots"));
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

    @Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Tuple record = new Tuple();
		record = parserRecords.parse(value.toString());
		Double distance = metric.distance(record, query);
		record.setDistance(distance);
		int partitionId = this.getIndexClosestPivot(record);
		PartitionDistancePair reducerKey = new PartitionDistancePair();
		reducerKey.setDistance(distance);
		reducerKey.setPartition(partitionId);
		TupleWritable tupleWritable = new TupleWritable(record);
		context.write(reducerKey, tupleWritable);
	}

    private void setPivots(String filePathPivots) throws IOException, IllegalArgumentException {
        this.pivots = new ArrayList<>();
        ParserTuple parserPivots = new ParserTuple();
        HDFSRead readPivots = new HDFSRead(filePathPivots);
        while(readPivots.hasNextLine()) {
            this.pivots.add(parserPivots.parse(readPivots.nextLine()));
        }
    }

    private Integer getIndexClosestPivot(Tuple tuple) {
        Integer index_closest_pivot = 0;
        Double current_distance = Double.MAX_VALUE;
        for(int i = 0; i < this.pivots.size(); i++) {
            Tuple pivot = pivots.get(i);
            Double distance = metric.distance(pivot, tuple);
            if(distance < current_distance) {
                index_closest_pivot = i;
                current_distance = distance;
            }
        }
        return index_closest_pivot;
    }
}
