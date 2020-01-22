package de.th_koeln.iws.sh2.aggregation.model;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DblpLogSession {

	private static final int ARBITRARY_SESSION_PAUSE_THRESHOLD = 30;

	private final List<DblpLog> logs;
	private final Set<String> uniqueGetRequests;
	private final List<String> swaps;

	private int circles = 0;

	private final long sessionId;
	private final long userId;

	public DblpLogSession(final long sessionId, final long userId) {

		this.logs = Lists.newArrayList();
		this.uniqueGetRequests = Sets.newHashSet();
		this.swaps = Lists.newArrayList();
		this.sessionId = sessionId;
		this.userId = userId;
	}

	public ImmutableList<DblpLog> getLogs() {
		return ImmutableList.sortedCopyOf(Comparator.comparing(DblpLog::getTimestamp), logs);
	}

	public long getSessionId() {
		return sessionId;
	}

	public long getUserId() {
		return userId;
	}

	public LocalDateTime getStart() {
		return logs.stream().map(l -> l.getTimestamp()).min(Comparator.naturalOrder()).orElse(null);
	}

	public LocalDateTime getEnd() {
		return logs.stream().map(l -> l.getTimestamp()).max(Comparator.naturalOrder()).orElse(null);
	}

	public long getTotalTime() {
		return (logs.size() == 0) ? 0 : getStart().until(getEnd(), ChronoUnit.SECONDS);
	}

	public int getCircles() {
		return circles;
	}

	public int getSwaps() {
		return swaps.size();
	}

	public boolean addDblpLog(DblpLog newLog) {
		if (logs.size() > 0) {
			if (logs.get(logs.size() - 1).getTimestamp().until(newLog.getTimestamp(),
					ChronoUnit.MINUTES) <= ARBITRARY_SESSION_PAUSE_THRESHOLD) {
				updateSession(newLog);
				return true;
			}
		} else {
			updateSession(newLog);
			return true;
		}
		return false;
	}

	private void updateSession(DblpLog newLog) {
		if (newLog.getGetRequest() != null && !uniqueGetRequests.add(newLog.getGetRequest()))
			circles++;
		if (logs.size() > 0 && newLog.getReferer() != null && !newLog.getReferer().contains("://dblp"))
			swaps.add(newLog.getReferer());
		logs.add(newLog);
	}

	public int getTotalPages() {
		return uniqueGetRequests.size() + circles;
	}

	public double getTimePerPage() {
		// TODO Auto-generated method stub
		return getTotalTime() * 1.0 / getTotalPages();
	}

}
