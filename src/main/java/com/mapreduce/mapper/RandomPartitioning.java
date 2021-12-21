package com.mapreduce.mapper;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.io.Text;
import com.types.PartitionDistancePair;
import com.types.TupleWritable;

/**
 * Mapper com partionamento aleatório. 
 * Correspondente a fase de particionamento.
 */
public class RandomPartitioning extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

	private Random rand = new Random();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		TupleWritable reducerValue = this.createReducerValue(value);
		this.emitCorePartition(reducerValue, context);
	}

	/**
     * Emite o reducerValue para a partição escolhida de maneira aleatória
     * @param reducerValue
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void emitCorePartition(TupleWritable reducerValue, Context context) throws IOException, InterruptedException {
		PartitionDistancePair reducerKey = new PartitionDistancePair();
		int partitionId = rand.nextInt(this.numberPartitions);
		reducerKey.setDistance(reducerValue.getDistance());
		reducerKey.setPartition(partitionId);
		context.write(reducerKey, reducerValue);
    }
}
