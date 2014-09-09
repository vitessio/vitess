package com.youtube.gorpc;

public class Response {

	private Header header;
	private Body body;

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	public Body getBody() {
		return body;
	}

	public void setBody(Body body) {
		this.body = body;
	}

	public class Header {
		public static final String SERVICE_METHOD = "ServiceMethod";
		public static final String SEQ = "Seq";
		public static final String ERROR = "Error";

		private String serviceMethod;
		private long seq;
		private String error;

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
	}

	public class Body {
		public static final String RESULT = "Result";
		public static final String ERROR = "Error";

		Object result;
		String error;

		public Object getResult() {
			return result;
		}

		public void setResult(Object result) {
			this.result = result;
		}

		public String getError() {
			return error;
		}

		public void setError(String error) {
			this.error = error;
		}
	}
}
