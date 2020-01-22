package de.th_koeln.iws.sh2.aggregation.model;

import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;

import de.th_koeln.iws.sh2.aggregation.core.xml.data.Type;
import de.th_koeln.iws.sh2.aggregation.model.util.ConvertStringToLocalDate;
import de.th_koeln.iws.sh2.aggregation.model.util.ConvertStringToMonth;
import de.th_koeln.iws.sh2.aggregation.model.util.ConvertStringToYear;
/**
 * Java Bean class representing a conference stream record.
 *
 * Used by the CSV reader (each row in the CSV is one record)
 *
 * @author mandy
 *
 */
public class StreamRecord implements Comparable<StreamRecord> {

	@CsvBindByName(column = "stream_key", required = true)
	private String streamKey;
	@CsvBindByName(column = "record_key", required = true)
	private String recordKey;
	@CsvCustomBindByName(column = "c_date", required = true, converter = ConvertStringToLocalDate.class)
	private LocalDate cDate;
	@CsvCustomBindByName(column = "record_evtmonth", required = true, converter = ConvertStringToMonth.class)
	private Month evtMonth;
	@CsvCustomBindByName(column = "record_evtyear", required = true, converter = ConvertStringToYear.class)
	private Year evtYear;
	@CsvBindByName(column = "ins_delay", required = true)
	private long insertDelayInMonths;
	private String title;
	private Year year;
	private Type type;
	private String evtPlace;

	public void setcDate(String cDate) {
		this.cDate = LocalDate.parse(cDate);
	}

	public void setEvtMonth(int evtMonth) {
		this.evtMonth = Month.of(evtMonth);
	}

	public void setEvtYear(int evtYear) {
		this.evtYear = Year.of(evtYear);
	}

	public void setInsertDelayInMonths(long delay) {
		this.insertDelayInMonths = delay;
	}

	public void setRecordKey(String recordKey) {
		this.recordKey = recordKey;
	}

	public void setStreamKey(String streamKey) {
		this.streamKey = streamKey;
	}

	public LocalDate getcDate() {
		return this.cDate;
	}

	public Month getEvtMonth() {
		return this.evtMonth;
	}

	public Year getEvtYear() {
		return this.evtYear;
	}

	public long getInsertDelayInMonths() {
		return this.insertDelayInMonths;
	}

	public String getRecordKey() {
		return this.recordKey;
	}

	public String getStreamKey() {
		return this.streamKey;
	}

	@Override
	public String toString() {
		return String.format("Record %s (stream: %s): created on %s, event was on %s, delay of %d", this.recordKey,
				this.streamKey, this.cDate, YearMonth.of(this.evtYear.getValue(), this.evtMonth).toString(),
				this.insertDelayInMonths);
	}

	public void setTitle(String title) {
		this.title = title;
		;
	}

	public void setYear(short year) {
		this.year = Year.of(year);
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getTitle() {
		return this.title;
	}

	public Type getType() {
		return this.type;
	}

	public Year getYear() {
		return this.year;
	}

	public void setEvtPlace(String evtPlace) {
		this.evtPlace = evtPlace;
	}

	public String getEvtPlace() {
		return this.evtPlace;
	}

	public YearMonth getEvtYearMonth() {
		return YearMonth.of(this.evtYear.getValue(), this.evtMonth.getValue());
	}

	@Override
	public int compareTo(StreamRecord o) {
		return this.recordKey.compareTo(o.recordKey);
	}
}