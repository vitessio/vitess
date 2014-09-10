package com.youtube.vitess.gorpc;

public class Request {

	private String serviceMethod;
	private long seq;

	public Request(String serviceMethod, long seq) {
		this.serviceMethod = serviceMethod;
		this.seq = seq;
	}

	public String getServiceMethod() {
		return serviceMethod;
	}

	public long getSeq() {
		return seq;
	}
}