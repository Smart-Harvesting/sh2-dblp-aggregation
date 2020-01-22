/**
 * 
 */
package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.COUNTRY_COUNT;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_COUNTRY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INTL_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_INTL_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.TOC_VIEW;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableMap;

/**
 * Aggregator for internationality scores of conferences.
 *
 * Makes use of streams per event view and historical records table (for place
 * information).
 *
 * Internationality score for each conference is calculated by dividing the
 * number of different countries in which the conference took place by the
 * number of total events of the conference.
 *
 * @author mandy
 *
 */
public class InternationalityAggregator extends CoreComponent {

	private static final Logger LOGGER = LogManager.getLogger(InternationalityAggregator.class);
	private static final int BATCH_SIZE = 1000;

	/**
	 * Wrapper method. Reads the required data, performs the calculation and
	 * batch-inserts the results in a new table.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 */
	public static void wrapTo(final Connection connection, final int lteYear) {

		final ImmutableMap<String, Integer> distinctCountriesPerStream = getNumberOfDistinctCountriesPerStream(
				connection, lteYear);
		final ImmutableMap<String, Integer> countryInfoPerStream = getNumberOfAllCountriesPerStream(connection,
				lteYear);

		reCreateTable(connection, RAW_INTL_SCORES, getCreateRawIntlScoresCmd());

		final String insertRawIntlScoresLogFormat = "{}: Batch-inserting into table '{}'";
		final Instant insertRawIntlScoresStart = Instant.now();
		LOGGER.info(insertRawIntlScoresLogFormat, "START", RAW_INTL_SCORES);

		int streamCount = 0;

		try (final PreparedStatement insertInternationalityScores = connection
				.prepareStatement(getInsertRawIntlScoresCmd())) {
			for (String streamKey : distinctCountriesPerStream.keySet()) {
				Integer distinctCountryCount = distinctCountriesPerStream.get(streamKey);
				if (!countryInfoPerStream.containsKey(streamKey)) {
					LOGGER.warn("No mapping in distinct countries per stream map for key " + streamKey);
					continue;
				}
				Integer totalCountryCount = countryInfoPerStream.get(streamKey);

				LOGGER.debug("[{}, {}] # distinct countries: {}", lteYear, streamKey, distinctCountryCount);
				LOGGER.debug("[{}, {}] # total countries: {}", lteYear, streamKey, totalCountryCount);

				final double rawScore = (totalCountryCount != 0) ? distinctCountryCount / (double) totalCountryCount
						: 0.0;

				LOGGER.debug("[{}, {}] # raw internationality score: {}", lteYear, streamKey, rawScore);

				insertInternationalityScores.setString(1, streamKey);
				insertInternationalityScores.setDouble(2, rawScore);

				insertInternationalityScores.addBatch();
				if ((++streamCount % BATCH_SIZE) == 0)
					insertInternationalityScores.executeBatch();
			}
			insertInternationalityScores.executeBatch();
		}

		catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertRawIntlScoresLogFormat, "FAILED", RAW_INTL_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertRawIntlScoresLogFormat + " (Duration: {})", "END", RAW_INTL_SCORES,
				Duration.between(insertRawIntlScoresStart, Instant.now()));
	}

	/**
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * @return mapping of stream key to number of distinct countries for events per
	 *         stream (up to including given year)
	 */
	private static ImmutableMap<String, Integer> getNumberOfDistinctCountriesPerStream(final Connection connection,
			final int lteYear) {
		final Map<String, Integer> distinctCountriesPerStream = new HashMap<>();

		final String aggregateLogFormat = "{}: Aggregating number of distinct countries per stream from table '{}'";
		final Instant insertStreamsStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", HISTORICAL_RECORDS);

		/*
		 * -- count number of countries per stream select events.stream_key,
		 * count(distinct hist.event_country) num_countries from
		 * dblp_conference_event_view events join dblp_historical_records hist on
		 * events.cite_key = hist.record_key group by events.stream_key;
		 */
		try (final PreparedStatement aggregateDistinctCountriesPerStream = connection
				.prepareStatement(getAggregateDistinctCountriesPerStreamCmd(lteYear))) {

			final ResultSet countriesPerStreamResults = aggregateDistinctCountriesPerStream.executeQuery();

			while (countriesPerStreamResults.next()) {
				final String streamKey = countriesPerStreamResults.getString(STREAM_KEY);
				final int countryCount = countriesPerStreamResults.getInt(COUNTRY_COUNT);

				distinctCountriesPerStream.put(streamKey, countryCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", TOC_VIEW), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", TOC_VIEW,
				Duration.between(insertStreamsStart, Instant.now()));

		return ImmutableMap.copyOf(distinctCountriesPerStream);
	}

	/**
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * @return mapping of stream key to number of events per stream (up to including
	 *         given year)
	 */
	private static ImmutableMap<String, Integer> getNumberOfAllCountriesPerStream(final Connection connection,
			final int lteYear) {
		final Map<String, Integer> countriesPerStream = new HashMap<>();

		final String aggregateLogFormat = "{}: Aggregating total number of countries per stream from table '{}'";
		final Instant insertStreamsStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", HISTORICAL_RECORDS);

		/*
		 * -- count number of events (records) per stream until 2015 select
		 * events.stream_key, count(distinct events.cite_key) num_events from
		 * dblp_conference_event_view events join dblp_historical_records hist on
		 * events.cite_key = hist.record_key where hist.event_year<=2015 group by
		 * events.stream_key ;
		 */
		try (final PreparedStatement aggregateAllCountriesPerStream = connection
				.prepareStatement(getAggregateAllCountriesPerStreamCmd(lteYear))) {

			final ResultSet eventsPerStreamResults = aggregateAllCountriesPerStream.executeQuery();

			while (eventsPerStreamResults.next()) {
				final String streamKey = eventsPerStreamResults.getString(STREAM_KEY);
				final int countryCount = eventsPerStreamResults.getInt(COUNTRY_COUNT);

				countriesPerStream.put(streamKey, countryCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", HISTORICAL_RECORDS), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", HISTORICAL_RECORDS,
				Duration.between(insertStreamsStart, Instant.now()));

		return ImmutableMap.copyOf(countriesPerStream);
	}

	private static String getAggregateDistinctCountriesPerStreamCmd(final int lteYear) {
		String aggregateDistinctCountriesPerStreamCmd = String.format(

				"select proceedings.%s, count(distinct hist.%s) %s "

						+ "from %s proceedings " + "join %s hist "

						+ "on proceedings.%s = hist.%s "

						+ "where hist.%s <= '%d-12-31' "

						+ "group by proceedings.%s" + ";",

				STREAM_KEY, EVENT_COUNTRY, COUNTRY_COUNT,

				PROCEEDINGS_VIEW, HISTORICAL_RECORDS,

				CITE_KEY, RECORD_KEY,

				C_DATE, lteYear,

				STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL",
				aggregateDistinctCountriesPerStreamCmd);
		return aggregateDistinctCountriesPerStreamCmd;
	}

	private static String getAggregateAllCountriesPerStreamCmd(final int lteYear) {
		String aggregateCountriesperStreamCmd = String.format(

				"select proceedings.%s, count(hist.%s) %s "

						+ "from %s proceedings " + "join %s hist "

						+ "on proceedings.%s = hist.%s "

						+ "where hist.%s <= '%d-12-31' "

						+ "group by proceedings.%s" + ";",

				STREAM_KEY, EVENT_COUNTRY, COUNTRY_COUNT,

				PROCEEDINGS_VIEW, HISTORICAL_RECORDS,

				CITE_KEY, RECORD_KEY,

				C_DATE, lteYear,

				STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", aggregateCountriesperStreamCmd);
		return aggregateCountriesperStreamCmd;
	}

	private static String getCreateRawIntlScoresCmd() {
		String createRawIntlScoresCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s numeric not null " + ");",

				RAW_INTL_SCORES,

				GENERIC_ID,

				STREAM_KEY, INTL_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createRawIntlScoresCmd);
		return createRawIntlScoresCmd;
	}

	private static String getInsertRawIntlScoresCmd() {
		String insertRawIntlScoresCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

				RAW_INTL_SCORES,

				STREAM_KEY, INTL_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertRawIntlScoresCmd);
		return insertRawIntlScoresCmd;
	}

}
