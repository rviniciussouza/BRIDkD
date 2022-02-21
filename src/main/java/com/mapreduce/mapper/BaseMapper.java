package com.mapreduce.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import com.helpers.ParserPoint;
import com.helpers.ParserTuple;
import com.helpers.WriterHDFS;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Point;
import com.types.Tuple;
import com.types.TupleWritable;

public abstract class BaseMapper<KeyIN, ValueIN, KeyOUT, ValueOUT> extends Mapper<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected ParserTuple parserRecords;
	protected Metric metric;
	protected int numberPartitions;
	protected Point query;

	/**
	 * If this method is overridden in a child class, it is necessary that
	 * method [[init]] be called before any declaration.
	 */
	@Override
	protected void setup(Context context) {
		this.init(context);
	}

	/**
	 * Inicializa o objeto com os parâmetros básicos.
	 */
	private void init(Context context) {
		this.setHeaderDataset(context.getConfiguration().getStrings("format.records"));
		this.numberPartitions = context.getConfiguration().getInt("mapper.number.partitions", 2);
		this.query = ParserPoint.parse(context.getConfiguration().get("brid.query"));
		this.setMetric();
	}

	/**
	 * Define a métrica a ser utilizada.
	 */
	protected void setMetric() {
		this.metric = new Eucledian();
	}

	/**
     * Defini uma versão serializada da tupla utilizando o tipo TupleWritable
     */
    public TupleWritable createReducerValue(Text value) {
		Tuple record = this.parserRecords.parse(value.toString());
        return new TupleWritable(record);
    }

	/**
	 * Defini o formato da tupla esperada
	 *  - Número de dimensões params[0],
	 *  - Flag indicando se o primeiro campo é um identificador params[1] 
	 *  - Flag indicando se há um campo de descrição da tupla params[2]
	 */
	protected void setHeaderDataset(String[] params) {
		parserRecords = new ParserTuple(
			Integer.parseInt(params[0]),
			Boolean.parseBoolean(params[1]),
			Boolean.parseBoolean(params[2])
		);
	}

	/**
     * Called once at the end of the task.
	 * 
	 * Escreve no arquivo de logs o número de vezes que a computação do cálculo de distâncias
	 * foi executado nessa tarefa.
     */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String outputFileName = context
			.getConfiguration()
			.get("experiment.logs.metric.calls")
			.concat(context.getTaskAttemptID().toString());
		WriterHDFS writer = new WriterHDFS(outputFileName);
		writer.write(Integer.toString(this.metric.numberOfCalculations));
		writer.close();
	}

}