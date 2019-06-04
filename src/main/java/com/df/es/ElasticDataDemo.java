package com.df.es;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Random;

public class ElasticDataDemo implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String name;
	private LocalDateTime created;
	private boolean quality;
	private int intValue;
	private double decValue;
	
	public ElasticDataDemo() {
		// TODO Auto-generated constructor stub
	}
	
	public ElasticDataDemo(String name) {
		this.name = name;
		this.created = LocalDateTime.now();
		this.quality = true;
		this.intValue = new Random(name.hashCode()).nextInt();
		this.decValue = new Random(name.hashCode()).nextDouble();
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getIntValue() {
		return intValue;
	}
	public void setIntValue(int intValue) {
		this.intValue = intValue;
	}
	public double getDecValue() {
		return decValue;
	}
	public void setDecValue(double decValue) {
		this.decValue = decValue;
	}
	public boolean isQuality() {
		return quality;
	}
	public void setQuality(boolean enabled) {
		this.quality = enabled;
	}
	public LocalDateTime getCreated() {
		return created;
	}
	public void setCreated(LocalDateTime created) {
		this.created = created;
	}
}
