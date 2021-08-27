package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.types.TupleWritable;


public class BridMapper extends BaseMapper<Object, Text, IntWritable, TupleWritable> {
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 
		IntWritable zero = new IntWritable(0);
		try {
			TupleWritable record = new TupleWritable();
			String line = value.toString().trim();
			record = read.getTuple(line);
			Double distance = metric.solve(record, query);
			record.setDistance(distance);			
			context.write(zero, record);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}