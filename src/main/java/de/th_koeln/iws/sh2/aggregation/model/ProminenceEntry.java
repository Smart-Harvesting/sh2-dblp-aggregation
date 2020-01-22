package de.th_koeln.iws.sh2.aggregation.model;

public class ProminenceEntry {

	private final String personKey;
	private final String streamKey;
	private final int year;
	private final int count;
	
	public ProminenceEntry(String personKey, String streamKey, int year, int count) {
		if (personKey == null || personKey.isEmpty())
			throw new IllegalArgumentException("Argument 'personKey' was '" + personKey + "': must not be null or empty");
		if (streamKey == null || streamKey.isEmpty())
			throw new IllegalArgumentException("Argument 'streamKey' was '" + streamKey + "': must not be null or empty");
		this.personKey = personKey;
		this.streamKey = streamKey;
		this.year = year;
		this.count = count;
	}

	public String getPersonKey() {
		return personKey;
	}

	public String getStreamKey() {
		return streamKey;
	}

	public int getYear() {
		return year;
	}

	public int getCount() {
		return count;
	}
	
}
