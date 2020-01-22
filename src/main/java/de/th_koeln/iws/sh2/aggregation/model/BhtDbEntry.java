package de.th_koeln.iws.sh2.aggregation.model;

public class BhtDbEntry {
	
	private final String bhtKey;
	private final String tocReference;
	private final int tocReferenceNumber;
	
	public BhtDbEntry(String bhtKey, String tocReference, int tocReferenceNumber) {
		if (bhtKey == null || bhtKey.isEmpty())
			throw new IllegalArgumentException("Argument 'bhtKey' was '" + bhtKey + "': must not be null or empty");
		if (tocReference == null || tocReference.isEmpty())
			throw new IllegalArgumentException("Argument 'tocReference' was '" + tocReference + "': must not be null or empty");
		this.bhtKey = bhtKey;
		this.tocReference = tocReference;
		this.tocReferenceNumber = tocReferenceNumber;
	}
	
	public String getBhtKey() {
		return bhtKey;
	}

	public String getTocReference() {
		return tocReference;
	}

	public int getTocReferenceNumber() {
		return tocReferenceNumber;
	}
	
}
