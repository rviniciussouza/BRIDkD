package com.mapreduce.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.helpers.ReaderHDFS;
import com.helpers.ParserPoint;
import com.types.PartitionDistancePair;
import com.types.Point;
import com.types.TupleWritable;


public class PivotBasedMapper extends BaseMapper<LongWritable, Text, PartitionDistancePair, TupleWritable> {

    protected List<Point> pivots;

    @Override
	protected void setup(Context context) {
		super.setup(context);
        try {
            this.setPivots(context.getConfiguration().get("mapper.pivots.file"));
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
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		TupleWritable reducerValue = this.createReducerValue(value);
        PartitionDistancePair reducerKey = this.processAsCorePoint(reducerValue);
		context.write(reducerKey, reducerValue);
	}

    public PartitionDistancePair processAsCorePoint(Point point) {
        int corePartitionId = this.getIndexClosestPivot(point);
        return this.generateOutputKey(corePartitionId, point);
    }

    public PartitionDistancePair generateOutputKey(int partition, Point point) {
        PartitionDistancePair reducerKey = new PartitionDistancePair(
            partition,
            this.metric.distance(point, this.query)
        );
        return reducerKey;
    }

    protected void setPivots(String filePathPivots) throws IOException, IllegalArgumentException {
        this.pivots = new ArrayList<>();
        ReaderHDFS readPivots = new ReaderHDFS(filePathPivots);
        while(readPivots.hasNextLine()) {
            this.pivots.add(ParserPoint.parse(readPivots.nextLine()));
        }
    }

    /**
     * Retorna o indíce do pivô mais próximo ao ponto p
     */
    protected int getIndexClosestPivot(Point p) {
        int indexClosesPivot = 0;
        double minimumDistance = Double.MAX_VALUE;
        for(int i = 0; i < this.pivots.size(); i++) {
            Point pivot = pivots.get(i);
            double distance = metric.distance(pivot, p);
            if(distance < minimumDistance) {
                indexClosesPivot = i;
                minimumDistance = distance;
            }
        }
        return indexClosesPivot;
    }
}
