package com.types;

import java.util.ArrayList;
import java.util.List;

public class Tuple {

	protected Long id;
	protected String description;
	protected List<Double> attributes;
	protected Double distance;

	public Tuple() {
		attributes = new ArrayList<>();
	}

	public Tuple(List<Double> attributes) {
		this.attributes = attributes;
	}

	public String asString() {
		return Long.toString(this.id);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Double getDistance() {
		return distance;
	}

	public void setDistance(Double distance) {
		this.distance = distance;
	}

	public void setAttributes(List<Double> attributes) {
		this.attributes = attributes;
	}

	public List<Double> getAttributes() {
		return this.attributes;
	}

	public void addAttribute(Double atr) {
		this.attributes.add(atr);
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		if(this.id != null) result.append(Long.toString(this.id) + "\t");
		for (Double value : this.attributes) {
			result.append(value + "\t");
		}
		if(this.description != null && !this.description.isEmpty()) result.append(this.description);
		return result.toString().trim();
	}
}
