package com.youtube.gorpc;

public class Request {
	public static final String SERVICE_METHOD = "ServiceMethod";
	public static final String SEQ = "Seq";

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