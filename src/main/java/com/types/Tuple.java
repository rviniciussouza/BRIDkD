package com.types;

import java.util.ArrayList;
import java.util.List;

public class Tuple extends Point {

	/**
	 * Identificador
	 */
	protected Long id;
	/**
	 * Descrição da tupla
	 */
	protected String description;
	/**
	 * Distância entre a tupla e o pivô mais próximo
	 */
	protected double distance;

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

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public void addAttribute(double atr) {
		this.attributes.add(atr);
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		if(this.id != null) result.append(Long.toString(this.id) + "\t");
		for (double value : this.attributes) {
			result.append(value + "\t");
		}
		if(this.description != null && !this.description.isEmpty()) result.append(this.description);
		return result.toString().trim();
	}
}
