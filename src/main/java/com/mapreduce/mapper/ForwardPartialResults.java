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
public class ForwardPartialResults extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Tuple record = parserRecords.parse(value.toString().trim());
		double distance = metric.distance(record, query);
		record.setDistance(distance);
		/** Todos os registros (resultados parciais) são encaminhados para um único reducer */
		PartitionDistancePair reducerKey = new PartitionDistancePair(
			0, distance
		);
		TupleWritable tupleWritable = new TupleWritable(record);
		context.write(reducerKey, tupleWritable);
	}
}