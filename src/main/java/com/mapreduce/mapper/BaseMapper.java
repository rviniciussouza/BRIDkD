package com.mapreduce.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import com.helpers.ParserTuple;
import com.helpers.WriterHDFS;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;
import com.types.TupleWritable;

public abstract class BaseMapper<KeyIN, ValueIN, KeyOUT, ValueOUT> extends Mapper<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected Tuple query;
	protected ParserTuple parserRecords;
	protected Integer numberPartitions;
	protected Metric metric;

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
		Configuration conf = context.getConfiguration();
		try {
			this.setFormatData(conf.getStrings("format.records"));
			ParserTuple parserQuery = new ParserTuple();
			this.query = parserQuery.parse(conf.get("brid.query"));
			this.numberPartitions = conf.getInt("mapper.number.partitions", 2);
			this.setMetric();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Define a métrica a ser utilizada
	 */
	protected void setMetric() {
		this.metric = new Eucledian();
	}

	/**
     * Defini uma versão serializada da tupla utilizando o tipo TupleWritable
     * @param value
     * @return TupleWritable
     */
    public TupleWritable createReducerValue(Text value) {
        Tuple record = new Tuple();
		record = this.parserRecords.parse(value.toString());
		double distance = metric.distance(record, query);
		record.setDistance(distance);
        TupleWritable reducerValue = new TupleWritable(record);
        return reducerValue;
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
		WriterHDFS writerDistanceCalculations = new WriterHDFS(
			conf.get("experiment.logs.metric.calls") + context.getTaskAttemptID()
		);
		writerDistanceCalculations.write(Integer.toString(this.metric.numberOfCalculations));
		writerDistanceCalculations.close();
	}

}