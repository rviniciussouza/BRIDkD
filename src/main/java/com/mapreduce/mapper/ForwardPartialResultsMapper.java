package com.mapreduce.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;
import com.types.TupleWritable;

/**
 * Classe responsável por obter e encaminhar os resultados parciais para
 * o nó (único) da fase de refinamento.
 */
public class ForwardPartialResultsMapper extends BaseMapper<Object, Text, IntWritable, TupleWritable> {

	protected Metric metric;
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		IntWritable zero = new IntWritable(0);
		Tuple record = new Tuple();
		String line = value.toString().trim();
		record = parserRecords.parse(line);
		Double distance = metric.solve(record, query);
		record.setDistance(distance);
		TupleWritable tupleWritable = new TupleWritable(record);
		context.write(zero, tupleWritable);
	}

	protected void setMetric() {
		this.metric = new Eucledian();
	}
}