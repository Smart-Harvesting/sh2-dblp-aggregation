package de.th_koeln.iws.sh2.aggregation.db;

public class Config {

	private String url;
	private String user;
	private String pw;

	public Config(String url, String user, String pass) {
		this.url = url;
		this.user = user;
		this.pw = pass;
	}

	public String getPw() {
		return this.pw;
	}

	public String getUrl() {
		return this.url;
	}

	public String getUser() {
		return this.user;
	}
}
