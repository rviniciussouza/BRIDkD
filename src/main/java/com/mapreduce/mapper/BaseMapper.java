package com.mapreduce.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import com.helpers.ParserTuple;
import com.helpers.WriterHDFS;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;

public abstract class BaseMapper<KeyIN, ValueIN, KeyOUT, ValueOUT> extends Mapper<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected Tuple query;
	protected ParserTuple parserRecords;
	protected Integer numberPartitions;
	protected Metric metric;

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		try {
			this.setFormatData(conf.getStrings("format.records"));
			ParserTuple parserQuery = new ParserTuple();
			this.query = parserQuery.parse(conf.get("brid.query"));
			this.numberPartitions = conf.getInt("mapper.number.partitions", 2);
			this.setMetric();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Define a métrica a ser utilizada
	 */
	protected void setMetric() {
		this.metric = new Eucledian();
	}

	/**
	 * Defini o formato de tupla esperado
	 *  - Número de dimensões params[0],
	 *  - Flag indicando se o primeiro campo é um identificador params[1] 
	 *  - Flag indicando se há um campo de descrição da tupla params[2]
	 * @param params
	 */
	protected void setFormatData(String[] params) {
		int dimension = Integer.parseInt(params[0]);
		Boolean have_id = Boolean.parseBoolean(params[1]);
		Boolean have_description = Boolean.parseBoolean(params[2]);
		parserRecords = new ParserTuple(dimension, have_id, have_description);
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