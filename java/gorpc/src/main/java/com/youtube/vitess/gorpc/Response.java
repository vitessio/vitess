package com.youtube.vitess.gorpc;

public class Response {
	private String serviceMethod;
	private long seq;
	private String error;
	private Object reply;

	public String getServiceMethod() {
		return serviceMethod;
	}

	public void setServiceMethod(String serviceMethod) {
		this.serviceMethod = serviceMethod;
	}

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public Object getReply() {
		return reply;
	}

	public void setReply(Object reply) {
		this.reply = reply;
	}
}
