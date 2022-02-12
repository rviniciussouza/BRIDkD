package com.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class TupleWritable extends Tuple implements WritableComparable<TupleWritable> {

	/*
	 * Defaults constructor to allow (de)serialization
	 */
	public TupleWritable() {
	}

	public TupleWritable(Tuple value) {
		this.id = value.id;
		this.description = value.description;
		this.distance = value.distance;
		this.attributes = value.attributes;
	}

	/*
	 * Serialize the fields of this object to out.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.id);
		out.writeUTF("" + this.description);
		out.writeDouble(this.distance);
		out.writeInt(this.attributes.size());
		for (double value : this.attributes) {
			out.writeDouble(value);
		}
	}

	/*
	 * Deserialize the fields of this object from in.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		this.description = in.readUTF();
		this.distance = in.readDouble();
		int size = in.readInt();
		this.attributes = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			this.attributes.add(in.readDouble());
		}
	}

	@Override
	public int compareTo(TupleWritable o) {
		return this.distance < o.distance ? -1 : this.distance == o.distance ? 0 : 1;
	}
}
