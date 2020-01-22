package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_COUNTRY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_DAY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_MONTH;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSERTION_DELAY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_MONTH;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_TITLE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_TYPE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.TOC_REF;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import de.th_koeln.iws.sh2.aggregation.core.xml.data.DblpEntry;
import de.th_koeln.iws.sh2.aggregation.core.xml.data.Type;


/**
 * Implementation of the {@link CoreComponent} to wrap hdblp data.
 *
 * @author mandy
 *
 */
public class HdblpDbWrapper extends CoreComponent {

	private static Logger LOGGER = LogManager.getLogger(HdblpDbWrapper.class);
	private static final int BATCH_SIZE = 100000;

	/**
	 * Parses the hdblp and writes all the entries to the database.
	 *
	 * @param connection     the database connection
	 * @param pathToHdblpXml the path to the hdblp xml
	 * @throws SQLException
	 */
	public static void wrapTo(final Connection connection) {

		wrapHistoricalRecordsTo(connection);

	}

	private static void wrapHistoricalRecordsTo(final Connection connection) {

		Map<String, DblpEntry> entries = ExternalDataHub.getHdblpProcessor().getEntries();
		LOGGER.info("Number of distinct entries: " + entries.size());

		reCreateTable(connection, HISTORICAL_RECORDS, getCreateHistoricalRecordsCmd(), true);

		final String insertLogFormat = "{}: Batch-inserting historical records into table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", HISTORICAL_RECORDS);

		int recordCount = 0;

		try (final PreparedStatement insertHistoricalRecord = connection
				.prepareStatement(getInsertHistoricalRecordCmd())) {

			for (DblpEntry entry : entries.values()) {

				if (entry.getType().equals(Type.WWW) == false) {
					insertHistoricalRecord.setString(1, entry.getKey());
					insertHistoricalRecord.setObject(2, entry.getCDate(), java.sql.Types.DATE);
					insertHistoricalRecord.setString(3, entry.getTocRef());
					insertHistoricalRecord.setString(4, entry.getTitle());
					insertHistoricalRecord.setString(5, entry.getType().getValue());
					insertHistoricalRecord.setObject(6, entry.getMonthValue(), java.sql.Types.SMALLINT);
					insertHistoricalRecord.setObject(7, entry.getYearValue(), java.sql.Types.SMALLINT);
					insertHistoricalRecord.setObject(8, entry.getEventMonthValue(), java.sql.Types.SMALLINT);
					insertHistoricalRecord.setObject(9, entry.getEventYearValue(), java.sql.Types.SMALLINT);
					insertHistoricalRecord.setObject(10, entry.getEventDayValue(), java.sql.Types.SMALLINT);
					insertHistoricalRecord.setObject(11, entry.getInsertDelay(), java.sql.Types.BIGINT);

					insertHistoricalRecord.addBatch();

					if ((++recordCount % BATCH_SIZE) == 0) {
						LOGGER.info("Processed {} historical records...", recordCount);
						insertHistoricalRecord.executeBatch();
					}
				}
			}
			insertHistoricalRecord.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", HISTORICAL_RECORDS), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", HISTORICAL_RECORDS,
				Duration.between(insertStart, Instant.now()));
	}

	private static String getCreateHistoricalRecordsCmd() {

		final String createHistoricalRecordsCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, " + "%s varchar(255) not null unique, " + "%s date not null, "

						+ "%s varchar(255), " + "%s varchar(2550) not null, " + "%s varchar(20) not null, "

						+ "%s smallint, " + "%s smallint, "

						+ "%s smallint, " + "%s smallint, " + "%s smallint, "

						+ "%s bigint, " + "%s varchar(255) " + ") " + ";",

						HISTORICAL_RECORDS,

						GENERIC_ID, RECORD_KEY, C_DATE,

						TOC_REF, RECORD_TITLE, RECORD_TYPE,

						PUBLICATION_MONTH, PUBLICATION_YEAR,

						EVENT_MONTH, EVENT_YEAR, EVENT_DAY,

						INSERTION_DELAY, EVENT_COUNTRY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createHistoricalRecordsCmd);
		return createHistoricalRecordsCmd;
	}

	private static String getInsertHistoricalRecordCmd() {

		final String insertHistoricalRecordCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" + " ) "

						+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" + ";",

						HISTORICAL_RECORDS,

						RECORD_KEY, C_DATE, TOC_REF, RECORD_TITLE, RECORD_TYPE,

						PUBLICATION_MONTH, PUBLICATION_YEAR,

						EVENT_MONTH, EVENT_YEAR, EVENT_DAY,

						INSERTION_DELAY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertHistoricalRecordCmd);
		return insertHistoricalRecordCmd;
	}

	// public static String getMaterializeRecordsPerProceedingsViewCmd() {
	// return String.format(
	//
	// "create materialized view %s as "
	//
	// + "select proceedings.%s, proceedings.%s, records.%s, proceedings.%s "
	//
	// + "from " + "(" + "select %s, %s, %s "
	//
	// + "from ( " + "select %s, %s, %s, %s, %s, %s "
	//
	// + "from %s events " + "join %s hist " + "on events.%s = hist.%s " + ")
	// conf_records "
	//
	// + "where conf_records.%s is not null " + "and conf_records.%s is not null "
	// + "and conf_records.%s like 'proceedings' "
	//
	// + "order by %s, %s " + ") proceedings "
	//
	// + "join %s records " + "on proceedings.%s = records.%s "
	//
	// + "order by %s, %s" + ";",
	//
	// RECORDS_PER_PROCEEDINGS_VIEW,
	//
	// STREAM_KEY, CITE_KEY, RECORD_KEY, EVENT_YEAR,
	//
	// STREAM_KEY, CITE_KEY, EVENT_YEAR,
	//
	// STREAM_KEY, CITE_KEY, RECORD_KEY, EVENT_MONTH, EVENT_YEAR, RECORD_TYPE,
	//
	// PROCEEDINGS_VIEW, HISTORICAL_RECORDS, CITE_KEY, RECORD_KEY,
	//
	// EVENT_MONTH, EVENT_YEAR, RECORD_TYPE,
	//
	// STREAM_KEY, CITE_KEY,
	//
	// CONFERENCE_RECORDS, CITE_KEY, CITE_KEY,
	//
	// STREAM_KEY, CITE_KEY);
	// }

}
