package com.mapreduce.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import com.helpers.ReaderHDFS;
import com.helpers.ParserTuple;
import com.types.PartitionDistancePair;
import com.types.Tuple;
import com.types.TupleWritable;


public class BasedPivotPartitionOverleapMapper extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

    protected List<Tuple> pivots;
    protected Double threshold;
    protected Integer queryPartition;
    
    @Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.setFormatData(conf.getStrings("format.records"));
		this.setMetric();
        try {
            this.setPivots(conf.get("mapper.pivots.file"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        ParserTuple parserQuery = new ParserTuple();
		this.query = parserQuery.parse(conf.get("brid.query"));
		this.numberPartitions = conf.getInt("mapper.number.partitions", 2);
        this.threshold = conf.getDouble("brid.threshold", 0.0);
        this.queryPartition = this.getQueryPartition();
	}

    @Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		TupleWritable reducerValue = this.createReducerValue(value);
		this.forwardCorePartition(reducerValue, context);
		this.forwardQueryPartition(reducerValue, context);
    }

    /**
     * Emite o reducerValue para a partição associada ao pivô mais próximo 
     * @param reducerValue
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void forwardCorePartition(TupleWritable reducerValue, Context context) throws IOException, InterruptedException {
		PartitionDistancePair reducerKey = new PartitionDistancePair();
		reducerKey.setDistance(reducerValue.getDistance());
        int corePartitionId = this.getIndexClosestPivot(reducerValue);
		reducerKey.setPartition(corePartitionId);
		context.write(reducerKey, reducerValue);
    }

    /**
     * Emite o reducerValue para a partição de consulta
     * sse o elemento está contido na região de janela
     * @param reducerValue
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void forwardQueryPartition(TupleWritable reducerValue, Context context) throws IOException, InterruptedException {
        if(inWindowRegion(reducerValue)) {
            PartitionDistancePair reducerKey = new PartitionDistancePair();
            reducerKey.setDistance(reducerValue.getDistance());
            reducerKey.setPartition(this.queryPartition);
            context.write(reducerKey, reducerValue);
        }
    }

    public TupleWritable createReducerValue(Text value) {
        Tuple records = new Tuple();
		records = this.parserRecords.parse(value.toString());
		Double distance = metric.distance(records, query);
		records.setDistance(distance);
        TupleWritable reducerValue = new TupleWritable(records);
        return reducerValue;
    }

    private Boolean inWindowRegion(Tuple t) {
        Tuple pivot0 = this.pivots.get(this.getIndexClosestPivot(t));
        Tuple pivot1 = this.pivots.get(this.queryPartition);
        Double hyperplanDist = this.metric.hyperplanDist(pivot0, pivot1, t);
        return hyperplanDist <= this.threshold;
    }

    private void setPivots(String filePathPivots) throws IOException, IllegalArgumentException {
        this.pivots = new ArrayList<>();
        ParserTuple parserPivots = new ParserTuple();
        ReaderHDFS readPivots = new ReaderHDFS(filePathPivots);
        while(readPivots.hasNextLine()) {
            this.pivots.add(parserPivots.parse(readPivots.nextLine()));
        }
    }

    /**
     * Obtem o identificador da partição de consulta
     * @return Integer
     */
    private Integer getQueryPartition() {
        return getIndexClosestPivot(this.query);
    }

    /**
     * Obtem o indice do pivô mais próximo á tupla passa por paramêtro
     * @param Tuple
     * @return Integer
    */
    private Integer getIndexClosestPivot(Tuple tuple) {
        Integer index_closest_pivot = 0;
        Double current_distance = Double.MAX_VALUE;
        for(int i = 0; i < this.pivots.size(); i++) {
            Tuple pivot = pivots.get(i);
            Double distance = metric.distance(pivot, tuple);
            if(distance < current_distance) {
                index_closest_pivot = i;
                current_distance = distance;
            }
        }
        return index_closest_pivot;
    }
}
