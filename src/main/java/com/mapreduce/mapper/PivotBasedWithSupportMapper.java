package com.mapreduce.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.helpers.WriterHDFS;
import com.types.PartitionDistancePair;
import com.types.Point;
import com.types.TupleWritable;

/**
 * Mapper - Particionamento baseado em pivô com pontos de suporte
 */
public class PivotBasedWithSupportMapper extends PivotBasedMapper {

    protected double factor;

    /**
     * Número de elementos duplicados entre as partições
     */
    protected int numberOfDuplicateObjects = 0;

    /**
     * @see Method setup of parent Class
     */
    @Override
    protected void setup(Context context) {
        super.setup(context);
        this.factor = context.getConfiguration().getDouble("brid.factor.f", 1.0);
    }

    /**
     * Kernel function map
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        TupleWritable reducerValue = this.createReducerValue(value);
        /* Handling core partition */
        PartitionDistancePair core = this.processAsCorePoint(reducerValue);
        context.write(core, reducerValue);
        /* Handling supported partitions */
        List<PartitionDistancePair> supported = this.processAsSupportPoint(reducerValue);
        for (PartitionDistancePair reducerKey : supported) {
            context.write(reducerKey, reducerValue);
            this.numberOfDuplicateObjects++;
        }
    }

    /**
     * Duplica o ponto para toda partição em que o ponto está contido
     * na região de suporte da partição.
     */
    public List<PartitionDistancePair> processAsSupportPoint(Point point) {
        List<PartitionDistancePair> supported = new ArrayList<>();
        int corePartitionId = this.getIndexClosestPivot(point);
        for (int partitionId = 0; partitionId < this.pivots.size(); partitionId++) {
            if (partitionId != corePartitionId &&
                    supportArea(point, pivots.get(corePartitionId), pivots.get(partitionId))) {
                supported.add(this.generateOutputKey(partitionId, point));
            }
        }
        return supported;
    }

    /**
     * Verifica se o objeto s está contido na área de suporte entre as partições
     * associadas ao pivôs p0 e p1.
     */
    protected boolean supportArea(Point s, Point p0, Point p1) {
        double distanceToHyperplane = metric.distHyperplane(p0, p1, s);
        return (distanceToHyperplane <= this.getThresold(p0, p1));
    }

    /**
     * Defini o threshold entre o pivo p0 e p1 automaticamente
     */
    public double getThresold(Point p0, Point p1) {
        double threshold = metric.distance(p0, p1) / 2.0;
        return (this.factor * threshold);
    }

    public int getnumberOfDuplicateObjects() {
        return this.numberOfDuplicateObjects;
    }

    /**
     * Called once at the end of the task.
     * 
     * Escreve no arquivo de logs o número de vezes que a computação do cálculo de
     * distâncias
     * foi executado nessa tarefa.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        String outputFileName = context
                .getConfiguration()
                .get("experiment.logs.duplicates")
                .concat(context.getTaskAttemptID().toString());
        WriterHDFS writer = new WriterHDFS(outputFileName);
        writer.write(Integer.toString(this.numberOfDuplicateObjects));
        writer.close();
    }

}
