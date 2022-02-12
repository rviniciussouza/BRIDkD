package com.mapreduce.mapper;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.helpers.TupleHelper;
import com.helpers.WriterHDFS;
import com.types.PartitionDistancePair;
import com.types.Tuple;
import com.types.TupleWritable;


/**
 * Mapper - Particionamento baseado em pivô com sobreposição
 */
public class PivotBasedOverlappingPartitioning extends PivotBasedPartitioning {

    private double threshold = .0;
    /**
     * Indíce da partição que contém o objeto de consulta
     */
    private Integer queryPartition;
    /**
     * Número de elementos duplicados para a partição de consulta
     */
    private Integer numberOfDuplicateElements;

    /**
     * @see Method setup of parent Class
     */
    @Override
	protected void setup(Context context) {
        super.setup(context);
		this.numberOfDuplicateElements = 0;
        this.setThreshold(context.getConfiguration().getInt("brid.factor.f", 1));
        this.queryPartition = this.getQueryPartition();
	}

    /**
     * Kernel function map
     */
    @Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		TupleWritable reducerValue = this.createReducerValue(value);
		this.emitCorePartition(reducerValue, context);
		this.emitQueryPartition(reducerValue, context);
    }

    /**
     * Insere o reducerValue para a partição associada ao pivô mais próximo 
     * @param reducerValue
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void emitCorePartition(TupleWritable reducerValue, Context context) throws IOException, InterruptedException {
		PartitionDistancePair reducerKey = new PartitionDistancePair();
		reducerKey.setDistance(reducerValue.getDistance());
        int corePartitionId = this.getIndexClosestPivot(reducerValue);
		reducerKey.setPartition(corePartitionId);
		context.write(reducerKey, reducerValue);
    }

    /**
     * Insere o reducerValue na partição de consulta se o elemento estiver contido na região de janela.
     * @param reducerValue
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void emitQueryPartition(TupleWritable reducerValue, Context context) throws IOException, InterruptedException {
        if(inWindowRegion(reducerValue)) {
            this.numberOfDuplicateElements++;
            PartitionDistancePair reducerKey = new PartitionDistancePair();
            reducerKey.setDistance(reducerValue.getDistance());
            reducerKey.setPartition(this.queryPartition);
            context.write(reducerKey, reducerValue);
        }
    }

    /**
     * Verifica se uma tupla está contida na região de janela
     * @param t
     * @return
     */
    private Boolean inWindowRegion(Tuple t) {
        Tuple pivot_kernel_t = this.pivots.get(this.getIndexClosestPivot(t));
        Tuple pivot_query_partition = this.pivots.get(this.queryPartition);
        if(!pivot_kernel_t.equals(pivot_query_partition)) {
            double hyperplanDist = this.metric.hyperplanDist(pivot_kernel_t, pivot_query_partition, t);
            return hyperplanDist <= this.threshold;
        }
        return false;
    }

    /**
     * Obtem o identificador da partição de consulta
     * @return Integer
     */
    private Integer getQueryPartition() {
        return this.getIndexClosestPivot(this.query);
    }

    /**
     * Define o valor do threshold baseado no fator f passando como argumento
     * Se o número de pivôs for menor que 2, o threshold será setado para 0
     * @param f
     * @throws IllegalArgumentException
     */
    public void setThreshold(int f) throws IllegalArgumentException {
        if(f <= 0) {
            throw new IllegalArgumentException("O valor do fator f deve ser maior ou igual á 1");
        }
        double middle = TupleHelper.middleDistance(this.pivots, this.metric);
        if(this.pivots.size() == 2) {
            this.threshold = (middle / (double)f);
        }
        else {
            this.threshold = (middle / (2 * (double)f));
        }
    }

    public Integer getNumberOfDuplicateElements() {
        return this.numberOfDuplicateElements;
    }

    /**
     * Called once at the end of the task.
	 * 
	 * Escreve no arquivo de logs o número de vezes que a computação do cálculo de distâncias
	 * foi executado nessa tarefa.
     */
    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		WriterHDFS writerDistanceCalculations = new WriterHDFS(
            conf.get("experiment.logs.metric.calls") + context.getTaskAttemptID()
        );
		writerDistanceCalculations.write(Integer.toString(this.metric.numberOfCalculations));
		writerDistanceCalculations.close();

		WriterHDFS writerDuplicateElements = new WriterHDFS(
            conf.get("experiment.logs.duplicates") + context.getTaskAttemptID()
        );
		writerDuplicateElements.write(Integer.toString(this.numberOfDuplicateElements));
		writerDuplicateElements.close();
	}
}
