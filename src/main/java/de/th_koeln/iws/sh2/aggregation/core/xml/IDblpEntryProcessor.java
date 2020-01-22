package de.th_koeln.iws.sh2.aggregation.core.xml;

/**
 *
 * Interface for a dblp entry processor.
 *
 * @author mandy
 *
 */
public interface IDblpEntryProcessor {

	/**
	 * Process the data of a dblp entry.
	 *
	 * @param keyValue entry key
	 * @param mDateValue entry mDate
	 * @param tocRef tocRef of entry
	 * @param title title of entry
	 * @param type entry type
	 * @param yearValue
	 */
	void process(String keyValue, String mDateValue, String tocRef, String title, String type, String yearValue, String monthValue);
}