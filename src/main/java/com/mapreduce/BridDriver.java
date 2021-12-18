package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.helpers.ReaderLocalFileSystem;
import com.mapreduce.grouping.PartitionDistanceGroupingComparator;
import com.mapreduce.mapper.ForwardPartialResultsMapper;
import com.mapreduce.mapper.RandomPartitionMapper;
import com.mapreduce.partitioner.PartitionDistancePartitioner;
import com.mapreduce.reducer.BridIntermediaryReducer;
import com.mapreduce.reducer.BridReducer;
import com.types.PartitionDistancePair;
import com.types.TupleWritable;

public class BridDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BridDriver(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		ReaderLocalFileSystem read = new ReaderLocalFileSystem(args[3]);
		Integer consulta = 0;
		
		while(read.hasNextLine()) {
			String query = read.nextLine();
			/* Configurações do JOB 1 */
			Configuration conf = getConf();
			conf.set("brid.query", query);
			System.out.println("\n");
			System.out.println("INICIANDO FASE DE PARTIONAMENTO");
			System.out.println("CONSULTA " + consulta); 
			System.out.println("\n");

			/* Configurações do JOB 1 */
			Configuration confOfPartitioner = getConf();			
			confOfPartitioner.set("brid.query", query);
			Job jobPartitioner = Job.getInstance(confOfPartitioner, "FIRST JOB");
			jobPartitioner.setJarByClass(BridDriver.class);
			jobPartitioner.setMapperClass(RandomPartitionMapper.class);
			jobPartitioner.setReducerClass(BridIntermediaryReducer.class);
			jobPartitioner.setPartitionerClass(PartitionDistancePartitioner.class);
			jobPartitioner.setGroupingComparatorClass(PartitionDistanceGroupingComparator.class);
			jobPartitioner.setMapOutputKeyClass(PartitionDistancePair.class);
			jobPartitioner.setMapOutputValueClass(TupleWritable.class);
			jobPartitioner.setOutputKeyClass(NullWritable.class);
			jobPartitioner.setOutputValueClass(Text.class);
			jobPartitioner.setNumReduceTasks(confOfPartitioner.getInt("mapper.number.partitions", 1));
			FileInputFormat.addInputPath(jobPartitioner, new Path(args[0]));
			FileOutputFormat.setOutputPath(jobPartitioner, new Path(args[1] + consulta + "/"));

			if (!jobPartitioner.waitForCompletion(true)) {
				System.exit(1);
			}

			System.out.println("\n");
			System.out.println("INICIANDO FASE DE REFINAMENTO");
			System.out.println("CONSULTA " + consulta); 
			System.out.println("\n");

			/* Configurações do JOB 2 */
			Configuration confOfRefinement = getConf();
			confOfRefinement.set("brid.query", query);
			Job jobRefinement = Job.getInstance(confOfRefinement, "SECOND JOB");
			jobRefinement.setJarByClass(BridDriver.class);
			jobRefinement.setMapperClass(ForwardPartialResultsMapper.class);
			jobRefinement.setReducerClass(BridReducer.class);
			jobRefinement.setMapOutputKeyClass(PartitionDistancePair.class);
			jobRefinement.setMapOutputValueClass(TupleWritable.class);
			jobRefinement.setPartitionerClass(PartitionDistancePartitioner.class);
			jobRefinement.setGroupingComparatorClass(PartitionDistanceGroupingComparator.class);
			jobRefinement.setOutputKeyClass(NullWritable.class);
			jobRefinement.setOutputValueClass(Text.class);
			jobRefinement.setNumReduceTasks(1);
			FileInputFormat.addInputPath(jobRefinement, new Path(args[1] + consulta));
			FileOutputFormat.setOutputPath(jobRefinement, new Path(args[2] + consulta));

			/* Aguardando o job completar para apagar os arquivos intermediarios */
			if (!jobRefinement.waitForCompletion(true)) {
				System.exit(1);
			}
			consulta++;
		}
		return 0;
	}
}