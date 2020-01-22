package de.th_koeln.iws.sh2.aggregation.model;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class DblpLog {

	private final long logId;
	private final String serverName;
	private final String clientHost;
	private final String logname;
	private final String clientUser;
	private final LocalDate date;
	private final LocalDateTime timestamp;
	private final String getRequest;
	private final int status;
	private final int bytes;
	private final String referer;
	private final String userAgent;

	public DblpLog(final long logId, final String serverName, final String clientHost, final String logname,
			final String clientUser, final LocalDate date, final LocalDateTime timestamp, final String getRequest,
			final int status, final int bytes, final String referer, String userAgent) {
		this.logId = logId;
		this.serverName = serverName;
		this.clientHost = clientHost;
		this.logname = logname;
		this.clientUser = clientUser;
		this.date = date;
		this.timestamp = timestamp;
		this.getRequest = getRequest;
		this.status = status;
		this.bytes = bytes;
		this.referer = referer;
		this.userAgent = userAgent;
	}

	public long getLogId() {
		return logId;
	}

	public String getServerName() {
		return serverName;
	}

	public String getClientHost() {
		return clientHost;
	}

	public String getLogname() {
		return logname;
	}

	public String getClientUser() {
		return clientUser;
	}

	public LocalDate getDate() {
		return date;
	}

	public LocalDateTime getTimestamp() {
		return timestamp;
	}

	public String getGetRequest() {
		return getRequest;
	}

	public int getStatus() {
		return status;
	}

	public int getBytes() {
		return bytes;
	}

	public String getReferer() {
		return referer;
	}

	public String getUserAgent() {
		return userAgent;
	}

}
