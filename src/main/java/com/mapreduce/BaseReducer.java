package com.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import com.helpers.ReadDataset;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;

public abstract class BaseReducer<KeyIN, ValueIN, KeyOUT, ValueOUT, T extends Tuple> extends Reducer<KeyIN, ValueIN, KeyOUT, ValueOUT> {
	

	protected T query;
	protected Metric<T> metric;
	protected ReadDataset read;
	protected Integer K;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		try {
			this.setFormatRecords(conf.getStrings("input_info"));
			this.query = (T)read.getTuple(conf.get("query"));
			this.K = conf.getInt("K", 5);
			this.defineMetric();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void defineMetric() {
		this.metric = new Eucledian<T>();
	}
	
	protected void setFormatRecords(String[] params) {
		int n_attributes = Integer.parseInt(params[0]);
		int has_id = Integer.parseInt(params[1]);
		int has_description = Integer.parseInt(params[2]);
		this.read = new ReadDataset(n_attributes, has_id, has_description);
	}
}