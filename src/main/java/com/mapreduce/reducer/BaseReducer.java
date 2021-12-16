package com.mapreduce.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import com.helpers.ParserTuple;
import com.helpers.WriterHDFS;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;

public abstract class BaseReducer<KeyIN, ValueIN, KeyOUT, ValueOUT>
		extends Reducer<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected Tuple query;
	protected Metric metric;
	protected Integer K;

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		ParserTuple parserQuery = new ParserTuple();
		this.query = parserQuery.parse(conf.get("brid.query"));
		this.K = conf.getInt("brid.K", 5);
		this.defineMetric();
	}

	protected void defineMetric() {
		this.metric = new Eucledian();
	}

	/**
     * Called once at the end of the task.
	 * 
	 * Escreve no arquivo de logs o número de vezes que a computação do cálculo de distâncias
	 * foi executado nessa tarefa.
     */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		WriterHDFS writer = new WriterHDFS(conf.get("experiment.logs.file") + context.getTaskAttemptID());
		writer.write(Integer.toString(this.metric.numberOfCalculations));
		writer.close();
	}
}