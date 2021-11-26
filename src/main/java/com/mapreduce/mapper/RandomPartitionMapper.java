package com.mapreduce.mapper;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.Text;

import com.types.PartitionDistancePair;
import com.types.Tuple;
import com.types.TupleWritable;

/**
 * Mapper com partionamento aleatório. 
 * Correspondente a fase de particionamento.
 */
public class RandomPartitionMapper extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

	private Random rand = new Random();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Tuple record = new Tuple();
		record = parserRecords.parse(value.toString());
		Double distance = metric.distance(record, query);
		record.setDistance(distance);
		int partitionId = rand.nextInt(this.numberPartitions);
		PartitionDistancePair reducerKey = new PartitionDistancePair();
		reducerKey.setDistance(distance);
		reducerKey.setPartition(partitionId);
		TupleWritable tupleWritable = new TupleWritable(record);
		context.write(reducerKey, tupleWritable);
	}
}
