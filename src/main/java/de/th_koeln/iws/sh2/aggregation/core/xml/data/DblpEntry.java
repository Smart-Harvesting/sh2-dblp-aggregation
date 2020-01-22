package de.th_koeln.iws.sh2.aggregation.core.xml.data;

import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

/**
 *
 * Class representing an entry of dblp.
 *
 * @author mandy
 *
 */
public class DblpEntry implements Comparable<DblpEntry> {

	private final String key;
	private LocalDate cDate;
	private final String tocRef;
	private final String title;
	private Type type;
	private Optional<Month> pubMonth;
	private Optional<Year> pubYear;
	private Optional<Month> evtMonth;
	private Optional<Year> evtYear;
	private Optional<Short> evtDay;
	private Long insertDelay;
	private String evtPlace;

	/**
	 * Constructor, creates a new DblpEntry object.
	 *
	 * @param keyValue
	 *            key of entry
	 * @param cDate
	 *            cdate of entry
	 * @param tocRef
	 *            tocRef of entry
	 * @param title
	 *            title of entry
	 * @param type
	 *            type of entry
	 * @param pubYear
	 */
	public DblpEntry(String keyValue, LocalDate cDate, String tocRef, String title, Type type, Optional<Year> year,
			Optional<Month> month) {
		this.key = keyValue;
		this.tocRef = tocRef;
		this.title = title;
		this.evtMonth = TitleDateParser.getEventMonth(title);
		this.evtYear = TitleDateParser.getEventYear(title);
		this.evtDay = TitleDateParser.getEventDay(title);
		this.type = type;
		this.pubYear = year;
		this.pubMonth = month;

		this.setCDate(cDate);
	}

	private void calculateAndSetInsertDelay() {
		if (this.evtMonth.isPresent() && this.evtYear.isPresent()) {
			YearMonth eventYM = YearMonth.of(this.evtYear.get().getValue(), this.evtMonth.get());
			YearMonth cDateYM = YearMonth.from(this.cDate);

			long delay = ChronoUnit.MONTHS.between(eventYM, cDateYM);

			this.insertDelay = new Long(delay);
		} else
			this.insertDelay = null;
	}

	/**
	 *
	 * @return key of entry
	 */
	public String getKey() {
		return this.key;
	}

	/**
	 * @return cDate of entry
	 */
	public LocalDate getCDate() {
		return this.cDate;
	}

	/**
	 * Set the creation date of this entry.
	 * Updates insertion delay in this process.
	 * @param date date to set the creation date to
	 */
	public void setCDate(LocalDate date) {
		this.cDate = LocalDate.from(date);
		this.calculateAndSetInsertDelay();
	}

	public Optional<Month> getMonth() {
		return this.pubMonth;
	}

	/**
	 * @return (publication) pubYear of entry
	 */
	public Optional<Year> getYear() {
		return this.pubYear;
	}

	/**
	 * @return tocRef of entry
	 */
	public String getTocRef() {
		return this.tocRef;
	}

	/**
	 * @return title of entry
	 */
	public String getTitle() {
		return this.title;
	}

	/**
	 * @return type of entry
	 */
	public Type getType() {
		return this.type;
	}

	@Override
	public String toString() {
		return String.format("Entry: %s -- created on %s", this.getKey(), this.getCDate().toString());
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 *
	 * Two entries are considered equal iff their keys are equal.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DblpEntry)
			return this.key.equals(((DblpEntry) obj).key);
		return false;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#hashCode()
	 *
	 * Calculate hashcode for keys
	 */
	@Override
	public int hashCode() {
		return this.key.hashCode();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 *
	 * For sorting, the keys are used
	 */
	@Override
	public int compareTo(DblpEntry o) {
		// TODO maybe more complex compare logic
		return this.key.compareTo(o.key);
	}

	/**
	 * @return the int value of the pubMonth (range 1--12), if present, otherwise
	 *         null.
	 */
	public Integer getMonthValue() {
		if (this.pubMonth.isPresent())
			return this.pubMonth.get().getValue();
		else
			return null;
	}

	/**
	 * @return the int value of the evtMonth (range 1--12), if present, otherwise
	 *         null.
	 */
	public Integer getEventMonthValue() {
		if (this.evtMonth.isPresent())
			return this.evtMonth.get().getValue();
		else
			return null;
	}

	/**
	 * @return the int value of the pubYear, if present, otherwise null.
	 */
	public Integer getYearValue() {
		if (this.pubYear.isPresent())
			return this.pubYear.get().getValue();
		else
			return null;
	}

	/**
	 * @return the int value of the evtYear, if present, otherwise null.
	 */
	public Integer getEventYearValue() {
		if (this.evtYear.isPresent())
			return this.evtYear.get().getValue();
		else
			return null;
	}

	public Short getEventDayValue() {
		if (this.evtDay.isPresent())
			return this.evtDay.get();
		else
			return null;
	}

	public Long getInsertDelay() {
		return this.insertDelay;
	}

	public String getEventPlace() {
		return this.evtPlace;
	}

}
