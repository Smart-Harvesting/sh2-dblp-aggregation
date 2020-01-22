package de.th_koeln.iws.sh2.aggregation.model;

public class StreamDbEntry {
	
	private final String streamKey;
	private final String bhtKey;
	private final double averageRank;
	
	public StreamDbEntry (String streamKey, String bhtKey, double averageRank) {
		if (streamKey == null || streamKey.isEmpty())
			throw new IllegalArgumentException("Argument 'streamKey' was '" + streamKey + "': must not be null or empty");
		if (bhtKey == null || bhtKey.isEmpty())
			throw new IllegalArgumentException("Argument 'bhtKey' was '" + bhtKey + "': must not be null or empty");
		this.streamKey = streamKey;
		this.bhtKey = bhtKey;
		this.averageRank = averageRank;
	}
	
	public String getStreamKey() {
		return streamKey;
	}

	public String getBhtKey() {
		return bhtKey;
	}

	public double getAverageRank() {
		return averageRank;
	}
	
}
