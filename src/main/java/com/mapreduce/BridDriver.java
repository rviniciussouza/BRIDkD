package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.helpers.ReadDataset;
import com.mapreduce.mapper.ForwardPartialResultsMapper;
import com.mapreduce.mapper.RandomPartitionMapper;
import com.mapreduce.reducer.BridIntermediaryReducer;
import com.mapreduce.reducer.BridReducer;
import com.types.TupleWritable;

public class BridDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BridDriver(), args);
        System.exit(res);
		System.exit(0);
	}

	@Override
	public int run(String[] args) throws Exception {
		ReadDataset read = new ReadDataset(args[5]);
		Integer consulta = 0;
		
		while(read.hasNextLine()) {
			String query = read.nextLine();
			/* Configurações do JOB 1 */
			Configuration conf = new Configuration();
			conf.set("query", query);
			conf.set("header_dataset", args[3]);
			conf.set("K", args[4]);
			conf.set("number_partitions", args[6]);
			System.out.println("Iniciando fase de partionamento");
			System.out.println("Consulta: " + consulta); 

			Job job = Job.getInstance(conf, "Brid_k_phase_one");
			job.setJarByClass(BridDriver.class);
			job.setMapperClass(RandomPartitionMapper.class);
			job.setReducerClass(BridIntermediaryReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(TupleWritable.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1] + consulta + "/"));

			if (!job.waitForCompletion(true)) {
				System.exit(1);
			}

			System.out.println("Iniciando fase de refinamento");
			System.out.println("Consulta: " + consulta); 

			/* Configurações do JOB 2 */
			Configuration conf2 = new Configuration();
			conf2.set("query", query);
			conf2.set("header_dataset", args[3]);
			conf2.set("K", args[4]);
			Job job2 = Job.getInstance(conf2, "Brid_k_phase_final");
			job2.setJarByClass(BridDriver.class);
			job2.setMapperClass(ForwardPartialResultsMapper.class);
			job2.setReducerClass(BridReducer.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(TupleWritable.class);

			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + consulta));
			FileOutputFormat.setOutputPath(job2, new Path(args[2] + consulta));

			/* Aguardando o job completar para apagar os arquivos intermediarios */
			if (!job2.waitForCompletion(true)) {
				System.exit(1);
			}
			consulta++;
		}
		return 0;
	}

	public static void deletePathIfExists(Configuration conf, String stepOutputPath) throws IOException {
		Path path = new Path(stepOutputPath);
		org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}