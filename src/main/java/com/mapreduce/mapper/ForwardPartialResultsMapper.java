package com.mapreduce.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import com.types.PartitionDistancePair;
import com.types.Tuple;
import com.types.TupleWritable;

/**
 * Classe responsável por obter e encaminhar os resultados parciais para
 * o nó (único) da fase de refinamento.
 */
public class ForwardPartialResultsMapper extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Tuple record = new Tuple();
		String line = value.toString().trim();
		record = parserRecords.parse(line);
		Double distance = metric.distance(record, query);
		record.setDistance(distance);
		PartitionDistancePair reducerKey = new PartitionDistancePair();
		reducerKey.setDistance(distance);
		reducerKey.setPartition(0);
		TupleWritable tupleWritable = new TupleWritable(record);
		context.write(reducerKey, tupleWritable);
	}
}