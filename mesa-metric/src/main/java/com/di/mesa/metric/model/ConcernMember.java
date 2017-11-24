package com.di.mesa.metric.model;

public class ConcernMember {

	private String name;
	private String tel;


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTel() {
		return tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	@Override
	public String toString() {
		return "ConcernMember [name=" + name + ", tel=" + tel + "]";
	}

}
