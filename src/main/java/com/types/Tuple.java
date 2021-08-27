package com.types;

import java.util.ArrayList;
import java.util.List;

public class Tuple {

    protected long id;
    protected String description;
    protected List<Double> attributes;
    protected Double distance;
    protected String raw;
    
    public Tuple() {
        attributes = new ArrayList<>();
    }

    public Tuple(List<Double> attributes) {
        this.attributes = attributes;
    }
    
    public String attributesAsString() {
    	String result = "";
    	for(Double value : this.attributes) {
    		result += value + " ";
    	}
    	return result.trim();
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

	public String getRaw() {
		return raw;
	}

	public void setRaw(String raw) {
		this.raw = raw;
	}
	
	@Override
	public String toString() {
		return this.raw;
	}
}
