package com.mapreduce.mapper;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import com.types.Tuple;
import com.types.TupleWritable;

/**
 * Mapper com partionamento aleatório. 
 * Correspondente a fase de particionamento.
 */
public class RandomPartitionMapper extends BaseMapper<Object, Text, IntWritable, TupleWritable> {

	private Random rand = new Random();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Tuple record = new Tuple();
		record = parserRecords.parse(value.toString());
		Double distance = metric.solve(record, query);
		record.setDistance(distance);
		int partitionId = rand.nextInt(this.numberPartitions);
		TupleWritable tupleWritable = new TupleWritable(record);
		context.write(new IntWritable(partitionId), tupleWritable);
	}
}
