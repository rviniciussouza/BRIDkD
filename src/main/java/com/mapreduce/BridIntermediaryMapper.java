package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import com.types.TupleWritable;

public class BridIntermediaryMapper extends BaseMapper<Object, Text, IntWritable, TupleWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
		
		try {
			TupleWritable record = new TupleWritable();
			record = read.getTuple(value.toString());
			Double distance = metric.solve(record, query);
			record.setDistance(distance);
			int red = (int)(record.getId() % 2);
			context.write(new IntWritable(red), record);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
