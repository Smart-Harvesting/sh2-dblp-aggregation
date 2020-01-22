package de.th_koeln.iws.sh2.aggregation.core.xml;

import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.th_koeln.iws.sh2.aggregation.core.xml.data.DblpEntry;
import de.th_koeln.iws.sh2.aggregation.core.xml.data.Type;

/**
 * Processor for entry data in hdblp.
 *
 * @author mandy
 *
 */
public class DblpEntryProcessor implements IDblpEntryProcessor {

	@SuppressWarnings("unused")
	private static final Logger LOGGER = LogManager.getLogger(DblpEntryProcessor.class);

	private static final DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

	/** Month regex string. */
	private static String monthRegex = "[Jj]anuary|[Ff]ebruary|[Mm]arch|[Aa]pril|[Mm]ay|[Jj]une"
			+ "|[Jj]uly|[Aa]ugust|[Ss]eptember|[Oo]ctober|[Nn]ovember|[Dd]ecember";

	private Map<String, DblpEntry> entries;

	/**
	 * Creates a new DblpEntryProcessor object.
	 */
	public DblpEntryProcessor() {
		this.entries = new HashMap<>();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * de.th_koeln.iws.sh2.monitoring.xml.IDblpEntryProcessor#process(java.lang.
	 * String, java.lang.String, java.lang.String, java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public void process(String keyValue, String mDateValue, String tocRef, String title, String typeValue,
			String yearValue, String monthValue) {
		keyValue = keyValue.intern(); // TODO usage?
		LocalDate mDate = this.parseMDateValue(mDateValue);
		Optional<Year> year = this.parseYearValue(yearValue);
		Optional<Month> month = this.parseMonthValue(monthValue);
		Type type = Type.fromString(typeValue);

		/*
		 * add or replace entry in map if current mDate is before the mDate of an
		 * already existing entry
		 */
		if (this.entries.containsKey(keyValue)) {
			DblpEntry existingEntry = this.entries.get(keyValue);
			if (mDate.isBefore(existingEntry.getCDate())) {
				existingEntry.setCDate(mDate);
			} else {
				this.entries.replace(keyValue,
						new DblpEntry(keyValue, existingEntry.getCDate(), tocRef, title, type, year, month));
			}
		} else {
			this.entries.put(keyValue, new DblpEntry(keyValue, mDate, tocRef, title, type, year, month));
		}
	}

	private Optional<Month> parseMonthValue(String monthValue) {
		if (monthValue == null) {
			return Optional.empty();
		}

		String[] parts = monthValue.split("[\\s/]");
		for (String part : parts) {
			if (part.matches(monthRegex)) {
				return Optional.of(Month.valueOf(part.toUpperCase()));
			}
		}
		return Optional.empty();
	}

	private Optional<Year> parseYearValue(String yearValue) {
		if (yearValue == null) {
			return Optional.empty();
		}
		try {
			if (yearValue.contains("/")) {
				return Optional.of(Year.parse(yearValue.split("/")[0]));
			}
			return Optional.of(Year.parse(yearValue));
		} catch (DateTimeParseException e) {
			return Optional.empty();
		}
	}

	private LocalDate parseMDateValue(String mDateValue) {
		return LocalDate.parse(mDateValue, ISO_LOCAL_DATE);
	}

	/**
	 * @return a mapping between an entry key and the corresponding
	 *         {@link de.th_koeln.iws.sh2.monitoring.xml.data.DblpEntry} object.
	 */
	public Map<String, DblpEntry> getEntries() {
		return this.entries;
	}
}
