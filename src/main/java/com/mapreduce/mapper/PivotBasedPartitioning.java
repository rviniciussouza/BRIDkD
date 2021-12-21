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


public class PivotBasedPartitioning extends BaseMapper<Object, Text, PartitionDistancePair, TupleWritable> {

    List<Tuple> pivots;

    @Override
	protected void setup(Context context) {
		super.setup(context);
        Configuration conf = context.getConfiguration();
        try {
            this.setPivots(conf.get("mapper.pivots.file"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
	}

    /**
     * Emite um par <chave, valor>, onde a chave corresponde a uma tupla do arquivo de entrada
     * e o valor corresponde a partição associada ao pivô mais próximo a está tupla.
     */
    @Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		TupleWritable reducerValue = this.createReducerValue(value);
		this.emitCorePartition(reducerValue, context);
	}

    /**
     * Emite o reducerValue para a partição associada ao pivô mais próximo 
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

    protected void setPivots(String filePathPivots) throws IOException, IllegalArgumentException {
        this.pivots = new ArrayList<>();
        ParserTuple parserPivots = new ParserTuple();
        ReaderHDFS readPivots = new ReaderHDFS(filePathPivots);
        while(readPivots.hasNextLine()) {
            this.pivots.add(parserPivots.parse(readPivots.nextLine()));
        }
    }

    protected Integer getIndexClosestPivot(Tuple tuple) {
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
