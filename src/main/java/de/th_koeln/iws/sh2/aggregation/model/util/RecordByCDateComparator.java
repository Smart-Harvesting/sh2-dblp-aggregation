package de.th_koeln.iws.sh2.aggregation.model.util;

import java.util.Comparator;

import de.th_koeln.iws.sh2.aggregation.model.StreamRecord;


/**
 * Comparator for stream records, basing comparison on cDates. If two records
 * are created on the same date, they are ordered by key.
 *
 * @author mandy
 *
 */
public class RecordByCDateComparator implements Comparator<StreamRecord> {
	@Override
	public int compare(StreamRecord o1, StreamRecord o2) {
		if (o1.equals(o2)) {
			return 0;
		}
		int cDateCompare = o1.getcDate().compareTo(o2.getcDate());
		if (cDateCompare == 0) {
			int objectCompare = o1.compareTo(o2);
			return objectCompare;
		} else
			return cDateCompare;
	}
}