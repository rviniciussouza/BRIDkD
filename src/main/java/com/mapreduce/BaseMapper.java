package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import com.helpers.ReadDataset;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;

public abstract class BaseMapper<KeyIN, ValueIN, KeyOUT, ValueOUT>  extends Mapper<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected Tuple query;
	protected Metric<Tuple> metric;
	protected ReadDataset read;
	
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		try {
			this.setFormatRecords(conf.getStrings("input_info"));
			this.query = read.getTuple(conf.get("query"));
			this.defineMetric();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void defineMetric() {
		this.metric = new Eucledian<>();
	}
	
	protected void setFormatRecords(String[] params) {
		int n_attributes = Integer.parseInt(params[0]);
		int has_id = Integer.parseInt(params[1]);
		int has_description = Integer.parseInt(params[2]);
		read = new ReadDataset(n_attributes, has_id, has_description);
	}
}