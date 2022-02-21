package com.types;

import java.util.ArrayList;
import java.util.List;

/**
 * Representa um ponto qualquer em um espaço métrico.
 */
public class Point {
    protected List<Double> attributes;

	public Point() {
		attributes = new ArrayList<>();
	}
	
	public Point(List<Double> attributes) {
		this.attributes = attributes;
	}

    public void setAttributes(List<Double> attributes) {
		this.attributes = attributes;
	}

	public List<Double> getAttributes() {
		return this.attributes;
	}
}
