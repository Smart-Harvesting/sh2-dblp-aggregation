package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.AFFILIATON_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITATION_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_COUNTRY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_DAY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_MONTH;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSERTION_DELAY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INTL_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PROMINENCE_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_TITLE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_TYPE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SIZE_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_AFFILIATON_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_CITATION_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_INTL_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_LOG_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_PROMINENCE_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_RATING_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_SIZE_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_VIEW;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Table;

import de.th_koeln.iws.sh2.aggregation.core.xml.data.Type;
import de.th_koeln.iws.sh2.aggregation.db.DatabaseManager;
import de.th_koeln.iws.sh2.aggregation.model.ConferenceStream;
import de.th_koeln.iws.sh2.aggregation.model.StreamRecord;



public class DbDataReader {

	private static Logger LOGGER = LogManager.getLogger(DbDataReader.class);

	private DatabaseManager dbm;

	private List<ConferenceStream> conferenceList;
	private List<StreamRecord> streamRecordList;
	private Multimap<String, StreamRecord> streamRecordMultimap;

	private Map<String, Double> streamRatingsMap;
	private Map<String, Double> streamCitationsMap;
	private Map<String, Double> streamProminenceMap;
	private Map<String, Double> streamInternationalityScores;
	private Map<String, Double> streamAffilScores;
	private Map<String, Double> streamSizesMap;
	Table<String, YearMonth, Double> streamLogScores;

	final String logFormat = "{}: Reading data from table '{}'";

	public DbDataReader(DatabaseManager dbm) {
		this.dbm = dbm;
		try {
			this.streamRecordList = this.read();
			this.readStreamRatings();
			this.readStreamCitations();
			this.readStreamProminence();
			this.readStreamInternationalityScores();
			this.readStreamSizes();
			this.readStreamAffiliationScores();
			this.readStreamLogScores();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void readStreamInternationalityScores() throws SQLException {

		LOGGER.info(this.logFormat, "START", RAW_INTL_SCORES);
		final Instant start = Instant.now();

		this.streamInternationalityScores = new TreeMap<>();
		String selectMaxString = String.format("SELECT MAX(%s) " + "FROM %s", INTL_SCORE, RAW_INTL_SCORES);
		String selectString = String.format("SELECT %s, %s " + "FROM %s;", STREAM_KEY, INTL_SCORE, RAW_INTL_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			double maxIntlScore = this.queryMaximum(selectMaxString);
			LOGGER.debug("Internationality maximum: {}", maxIntlScore);

			/*
			 * get all results
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				String streamKey = rs.getString(STREAM_KEY);
				Double intScore = rs.getDouble(INTL_SCORE);
				LOGGER.debug("[{}] raw intl score: {}", streamKey, intScore);
				intScore = intScore / maxIntlScore;
				LOGGER.debug("[{}] averaged intl score: {}", streamKey, intScore);
				if (null != this.streamInternationalityScores.put(streamKey, intScore)) {
					LOGGER.error("Stream key is not unique in internationality score table!");
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_INTL_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
	}

	private void readStreamAffiliationScores() throws SQLException {

		LOGGER.info(this.logFormat, "START", RAW_AFFILIATON_SCORES);
		final Instant start = Instant.now();

		this.streamAffilScores = new TreeMap<>();
		String selectMaxString = String.format("SELECT MAX(%s) " + "FROM %s", AFFILIATON_SCORE, RAW_AFFILIATON_SCORES);
		String selectString = String.format("SELECT %s, %s " + "FROM %s;", STREAM_KEY, AFFILIATON_SCORE,
				RAW_AFFILIATON_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			double maxAffilScore = this.queryMaximum(selectMaxString);
			LOGGER.debug("Affiliation maximum: {}", maxAffilScore);

			/*
			 * get all results
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				String streamKey = rs.getString(STREAM_KEY);
				Double affilScore = rs.getDouble(AFFILIATON_SCORE);
				LOGGER.debug("[{}] raw affil score: {}", streamKey, affilScore);
				affilScore = affilScore / maxAffilScore;
				LOGGER.debug("[{}] averaged affil score: {}", streamKey, affilScore);
				if (null != this.streamAffilScores.put(streamKey, affilScore)) {
					LOGGER.error("Stream key is not unique in affiliations score table!");
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_INTL_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
	}

	private void readStreamLogScores() throws SQLException {

		LOGGER.info(this.logFormat, "START", RAW_LOG_SCORES);
		final Instant start = Instant.now();

		this.streamLogScores = HashBasedTable.create();

		String selectString = String.format("SELECT * " + "FROM %s;", RAW_LOG_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			/*
			 * get all results
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			Map<String, YearMonth> yearMonthsColumnMap = this.getYearMonthsOfLogs(rs);
			Map<String, Double> maxPerColumn = this.getMaxPerColumn(yearMonthsColumnMap.keySet());

			while (rs.next()) {

				String streamKey = rs.getString(STREAM_KEY);

				for (String columnName : yearMonthsColumnMap.keySet()) {
					Double logScore = rs.getDouble(columnName);
					LOGGER.debug("[{}, {}] raw log score: {}", streamKey, columnName, logScore);

					// teilen durch max
					logScore = logScore / maxPerColumn.get(columnName);
					LOGGER.debug("[{}, {}] averaged log score: {}", streamKey, columnName, logScore);

					this.streamLogScores.put(streamKey, yearMonthsColumnMap.get(columnName), logScore);
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_LOG_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
	}

	private Map<String, YearMonth> getYearMonthsOfLogs(ResultSet rs) throws SQLException {
		Map<String, YearMonth> monthsColumnMap = new HashMap<>();

		Pattern yearPattern = Pattern.compile("(?<=y)(19|20)\\d{2}(?=m)");
		Pattern monthPattern = Pattern.compile("(?<=m)\\d{2}$");

		Matcher yearMatcher = yearPattern.matcher("");
		Matcher monthMatcher = monthPattern.matcher("");

		String year, month;

		int columnCount = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			String columnName = rs.getMetaData().getColumnName(i);
			yearMatcher.reset(columnName);
			if (yearMatcher.find()) {
				year = yearMatcher.group();
				monthMatcher.reset(columnName);
				if (monthMatcher.find()) {
					month = monthMatcher.group();
					YearMonth ym = YearMonth.of(Integer.parseInt(year), Integer.parseInt(month));
					monthsColumnMap.put(columnName, ym);
				}
			}

		}
		return monthsColumnMap;

	}

	private Map<String, Double> getMaxPerColumn(Set<String> columns) throws SQLException {
		Map<String, Double> maxPerColumn = new HashMap<>();

		String selectTemplate = "SELECT MAX(%s) " + "FROM %s";

		for (String column : columns) {
			String selectMaxString = String.format(selectTemplate, column, RAW_LOG_SCORES);
			Double max = this.queryMaximum(selectMaxString);
			maxPerColumn.put(column, max);
		}

		return maxPerColumn;
	}

	private Double queryMaximum(String selectMaxString) throws SQLException {
		final Instant start = Instant.now();

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			/*
			 * get max value
			 */
			stmt.executeQuery(selectMaxString);
			connection.commit();
			rs = stmt.getResultSet();

			double maxValue = -1.0;
			// get the single value returned by that statement
			if (rs.next()) {
				maxValue = rs.getDouble(1);
				return maxValue;
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info("{} Reading maximum" + " (Duration: {})", "END", Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
		}
		return null;
	}

	private void readStreamProminence() throws SQLException {

		LOGGER.info(this.logFormat, "START", RAW_PROMINENCE_SCORES);
		final Instant start = Instant.now();

		this.streamProminenceMap = new TreeMap<>();
		String selectMaxProminenceString = String.format("SELECT MAX(%s) " + "FROM %s", PROMINENCE_SCORE,
				RAW_PROMINENCE_SCORES);
		String selectString = String.format("SELECT %s, %s " + "FROM %s;", STREAM_KEY, PROMINENCE_SCORE,
				RAW_PROMINENCE_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();
			/*
			 * get max prominence value
			 */
			double maxProminence = this.queryMaximum(selectMaxProminenceString);
			LOGGER.debug("Prominence maximum: {}", maxProminence);

			/*
			 * get all results
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				String streamKey = rs.getString(STREAM_KEY);
				Double prominence = rs.getDouble(PROMINENCE_SCORE);
				LOGGER.debug("[{}] raw prominence score: {}", streamKey, prominence);
				prominence = prominence / maxProminence;
				LOGGER.debug("[{}] averaged prominence score: {}", streamKey, prominence);
				if (null != this.streamProminenceMap.put(streamKey, prominence)) {
					LOGGER.error("Stream key is not unique in prominence score table!");
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_PROMINENCE_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}

	}

	private void readStreamCitations() throws SQLException {

		LOGGER.info(this.logFormat, "START", RAW_CITATION_SCORES);
		final Instant start = Instant.now();

		this.streamCitationsMap = new TreeMap<>();
		String selectMaxCitationString = String.format("SELECT MAX(%s) " + "FROM %s;", CITATION_SCORE,
				RAW_CITATION_SCORES);
		String selectString = String.format("SELECT %s, %s " + "FROM %s;", STREAM_KEY, CITATION_SCORE,
				RAW_CITATION_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			/*
			 * get max citation value
			 */
			double maxCitation = this.queryMaximum(selectMaxCitationString);
			LOGGER.debug("Citation maximum: {}", maxCitation);

			/*
			 * get all results
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				String streamKey = rs.getString(STREAM_KEY);
				Double citations = rs.getDouble(CITATION_SCORE);
				LOGGER.debug("[{}] raw citation score: {}", streamKey, citations);
				citations = citations / maxCitation;
				LOGGER.debug("[{}] averaged citation score: {}", streamKey, citations);
				if (null != this.streamCitationsMap.put(streamKey, citations)) {
					LOGGER.error("Stream key is not unique in citations scores table!");
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_CITATION_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
	}

	private void readStreamRatings() throws SQLException {
		LOGGER.info(this.logFormat, "START", RAW_RATING_SCORES);
		final Instant start = Instant.now();

		this.streamRatingsMap = new TreeMap<>();

		String selectMaxRatingString = String.format("SELECT MAX(%s) " + "FROM %s;", RATING_SCORE, RAW_RATING_SCORES);
		String selectString = String.format("SELECT %s, %s " + "FROM %s;", STREAM_KEY, RATING_SCORE, RAW_RATING_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			/*
			 * get max ranking
			 */
			double maxRating = this.queryMaximum(selectMaxRatingString);
			LOGGER.debug("Rating maximum: {}", maxRating);

			/*
			 * get all streams
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				String streamKey = rs.getString(STREAM_KEY);
				Double avgRating = rs.getDouble(RATING_SCORE);
				LOGGER.debug("[{}] raw ratings score: {}", streamKey, avgRating);
				avgRating = avgRating / maxRating;
				LOGGER.debug("[{}] averaged ratings score: {}", streamKey, avgRating);
				if (null != this.streamRatingsMap.put(streamKey, avgRating)) {
					LOGGER.error("Stream key is not unique in rating scores table!");
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_RATING_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
	}

	private void readStreamSizes() throws SQLException {
		LOGGER.info(this.logFormat, "START", RAW_RATING_SCORES);
		final Instant start = Instant.now();

		this.streamSizesMap = new TreeMap<>();

		String selectMaxString = String.format("SELECT MAX(%s) " + "FROM %s;", SIZE_SCORE, RAW_SIZE_SCORES);
		String selectString = String.format("SELECT %s, %s " + "FROM %s;", STREAM_KEY, SIZE_SCORE, RAW_SIZE_SCORES);

		final Connection connection = this.dbm.getConnection();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			/*
			 * get max avg size
			 */

			double maxSize = this.queryMaximum(selectMaxString);
			LOGGER.debug("Size maximum: {}", maxSize);

			/*
			 * get all streams
			 */
			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				String streamKey = rs.getString(STREAM_KEY);
				Double avgSize = rs.getDouble(SIZE_SCORE);
				LOGGER.debug("[{}] raw size score: {}", streamKey, avgSize);
				avgSize = avgSize / maxSize;
				LOGGER.debug("[{}] averaged size score: {}", streamKey, avgSize);
				if (null != this.streamSizesMap.put(streamKey, avgSize)) {
					LOGGER.error("Stream key is not unique in size scores table!");
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_RATING_SCORES,
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
	}

	private List<StreamRecord> read() throws SQLException {

		LOGGER.info(this.logFormat, "START", "joined table");
		final Instant start = Instant.now();

		final List<StreamRecord> toReturn = new ArrayList<>();

		Statement stmt = null;
		ResultSet rs = null;

		/*
		 * we only get streams and their records where the event month and year of the
		 * record is known, as we need this information to estimate the delay between
		 * conference and entry to dblp later on
		 */
		String innerSelect = String.format(
				"SELECT procs.%s, procs.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s "
						+ "FROM %s procs " + "JOIN %s hist " + "ON procs.%s = hist.%s",

						STREAM_KEY, CITE_KEY, C_DATE, RECORD_TITLE, RECORD_TYPE, PUBLICATION_YEAR, EVENT_MONTH, EVENT_YEAR,
						EVENT_DAY, EVENT_COUNTRY, INSERTION_DELAY,

						PROCEEDINGS_VIEW,

						HISTORICAL_RECORDS,

						CITE_KEY, RECORD_KEY);

		String selectString = String.format(
				"SELECT * " + "FROM (%s) AS conf_records " + "WHERE conf_records.%s IS NOT null "
						+ "AND conf_records.%s IS NOT null " + "AND conf_records.%s IS NOT null",

						innerSelect,

						EVENT_MONTH,

						EVENT_YEAR,

						EVENT_DAY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectString);

		final Connection connection = this.dbm.getConnection();

		try {
			connection.setAutoCommit(false);

			stmt = connection.createStatement();

			stmt.executeQuery(selectString);
			connection.commit();

			rs = stmt.getResultSet();

			while (rs.next()) {
				Date cDate = rs.getDate(C_DATE);
				short evtMonth = rs.getShort(EVENT_MONTH);
				short evtYear = rs.getShort(EVENT_YEAR);
				long delay = rs.getLong(INSERTION_DELAY);
				String recordKey = rs.getString(CITE_KEY);
				String streamKey = rs.getString(STREAM_KEY);
				String title = rs.getString(RECORD_TITLE);
				String type = rs.getString(RECORD_TYPE);
				short year = rs.getShort(PUBLICATION_YEAR);
				String evtPlace = rs.getString(EVENT_COUNTRY);

				StreamRecord record = new StreamRecord();
				record.setcDate(cDate.toString());
				record.setEvtMonth(evtMonth);
				record.setEvtYear(evtYear);
				record.setInsertDelayInMonths(delay);
				record.setStreamKey(streamKey);
				record.setRecordKey(recordKey);
				record.setTitle(title);
				record.setYear(year);
				record.setType(Type.fromString(type));
				record.setEvtPlace(evtPlace);

				LOGGER.debug("Read stream record: {}", record);

				toReturn.add(record);
			}
		} catch (SQLException e) {
			// JDBCTutorialUtilities.printSQLException(e);
			e.printStackTrace();
			if (connection != null) {
				try {
					LOGGER.error("Transaction is being rolled back");
					connection.rollback();
				} catch (SQLException excep) {
					// JDBCTutorialUtilities.printSQLException(excep);
					e.printStackTrace();
				}
			}
		} finally {
			LOGGER.info(this.logFormat + " (Duration: {})", "END", "joined table",
					Duration.between(start, Instant.now()));
			if (stmt != null)
				stmt.close();
			connection.setAutoCommit(true);
		}
		return toReturn;
	}

	/**
	 * @return the read data as Multimap
	 */
	public Multimap<String, StreamRecord> getAsMultiMap() {
		if (null == this.streamRecordMultimap) {
			// build a mapping from stream key to a collection of records
			this.streamRecordMultimap = MultimapBuilder.hashKeys().arrayListValues().build();
			for (StreamRecord streamRecord : this.streamRecordList) {
				boolean put = this.streamRecordMultimap.put(streamRecord.getStreamKey(), streamRecord);
				if (!put)
					LOGGER.debug("Record has not been added to key-record map: " + streamRecord);
			}
		}
		return this.streamRecordMultimap;
	}

	/**
	 * @return the read data as a List of ConferenceStreams
	 */
	public List<ConferenceStream> getAsListOfStreams() {
		if (null == this.conferenceList) {
			this.conferenceList = new ArrayList<>();
			for (String streamKey : this.getAsMultiMap().keySet()) {
				Collection<StreamRecord> records = this.streamRecordMultimap.get(streamKey);
				ConferenceStream conf = new ConferenceStream(streamKey, records);
				conf.setRatingScore(this.streamRatingsMap.get(streamKey));
				conf.setIntlScore(this.streamInternationalityScores.get(streamKey));
				conf.setCitScore(this.streamCitationsMap.get(streamKey));
				conf.setPromScore(this.streamProminenceMap.get(streamKey));
				conf.setSizeScore(this.streamSizesMap.get(streamKey));
				conf.setAffilScore(this.streamAffilScores.get(streamKey));
				conf.setLogScores(this.streamLogScores.row(streamKey));
				this.conferenceList.add(conf);
			}
		}
		LOGGER.debug("Size of conference list: " + this.conferenceList.size());
		return this.conferenceList;
	}

	/**
	 * @return the read data as a Set of ConferenceStreams
	 */
	public Set<ConferenceStream> getAsSetOfStreams() {
		TreeSet<ConferenceStream> conferenceSet = new TreeSet<>(this.getAsListOfStreams());

		LOGGER.debug("Size of conference set: " + conferenceSet.size());

		return conferenceSet;
	}
}
