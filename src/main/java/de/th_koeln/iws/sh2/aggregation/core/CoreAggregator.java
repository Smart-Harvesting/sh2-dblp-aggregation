package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.AUTHOR_COUNT;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_COUNT;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_AUTHOR_COUNT_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_RECORD_COUNT_VIEW;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.TreeBasedTable;

public class CoreAggregator extends CoreComponent {

	/**
	 * Helper method to get the number of distinct authors of an event.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code CITE_KEY}s; cell values = number of distinct
	 *         authors)
	 */
	protected static ImmutableTable<String, String, Integer> getAuthorsPerCite(final Connection connection,
			final int lteYear) {

		final TreeBasedTable<String, String, Integer> authorsPerCite = TreeBasedTable.create();

		final String aggregateLogFormat = "{}: Aggregating authors per cite from view '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", PROCEEDINGS_AUTHOR_COUNT_VIEW);

		try (final PreparedStatement aggregateAuthorsPerCite = connection
				.prepareStatement(getAuthorsPerEventCmd(lteYear))) {

			final ResultSet authorsPerCiteResults = aggregateAuthorsPerCite.executeQuery();

			while (authorsPerCiteResults.next()) {

				final String streamKey = authorsPerCiteResults.getString(STREAM_KEY);
				final String citeKey = authorsPerCiteResults.getString(CITE_KEY);
				final int authorCount = authorsPerCiteResults.getInt(AUTHOR_COUNT);

				authorsPerCite.put(streamKey, citeKey, authorCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", PROCEEDINGS_AUTHOR_COUNT_VIEW), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", PROCEEDINGS_AUTHOR_COUNT_VIEW,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableTable.copyOf(authorsPerCite);
	}

	/**
	 * Helper method to get the number of distinct authors of an event.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code PUBLICATION_YEAR}s; cell values = number of
	 *         distinct authors)
	 */
	protected static ImmutableTable<String, Integer, Integer> getAuthorsPerPublYear(final Connection connection,
			final int lteYear) {

		final TreeBasedTable<String, Integer, Integer> authorsPerPublYear = TreeBasedTable.create();

		final String aggregateLogFormat = "{}: Aggregating authors per publication year from view '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", PROCEEDINGS_AUTHOR_COUNT_VIEW);

		try (final PreparedStatement aggregateAuthorsPerPublYear = connection
				.prepareStatement(getAuthorsPerEventCmd(lteYear))) {

			final ResultSet authorsPerPublYearResults = aggregateAuthorsPerPublYear.executeQuery();

			while (authorsPerPublYearResults.next()) {

				final String streamKey = authorsPerPublYearResults.getString(STREAM_KEY);
				final int publicationYear = authorsPerPublYearResults.getInt(PUBLICATION_YEAR);
				final int authorCount = authorsPerPublYearResults.getInt(AUTHOR_COUNT);

				authorsPerPublYear.put(streamKey, publicationYear, authorCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", PROCEEDINGS_AUTHOR_COUNT_VIEW), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", PROCEEDINGS_AUTHOR_COUNT_VIEW,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableTable.copyOf(authorsPerPublYear);
	}

	/**
	 * Helper method to get the number of records of an event.
	 *
	 * @param connection the database connection
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code CITE_KEY}s; cell values = number of records)
	 */
	protected static ImmutableTable<String, String, Integer> getRecordsPerCite(final Connection connection,
			final int lteYear) {

		final TreeBasedTable<String, String, Integer> recordsPerEvent = TreeBasedTable.create();

		final String aggregateLogFormat = "{}: Aggregating records per cite from view '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", PROCEEDINGS_RECORD_COUNT_VIEW);

		try (final PreparedStatement aggregateRecordsPerCite = connection
				.prepareStatement(getRecordsPerEventCmd(lteYear))) {

			final ResultSet recordsPerCiteResults = aggregateRecordsPerCite.executeQuery();

			while (recordsPerCiteResults.next()) {

				final String streamKey = recordsPerCiteResults.getString(STREAM_KEY);
				final String citeKey = recordsPerCiteResults.getString(CITE_KEY);
				final int recordCount = recordsPerCiteResults.getInt(RECORD_COUNT);

				recordsPerEvent.put(streamKey, citeKey, recordCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", PROCEEDINGS_RECORD_COUNT_VIEW), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", PROCEEDINGS_RECORD_COUNT_VIEW,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableTable.copyOf(recordsPerEvent);
	}

	/**
	 * Helper method to get the number of records of an event.
	 *
	 * @param connection the database connection
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code PUBLICATION_YEAR}s; cell values = number of
	 *         records)
	 */
	protected static ImmutableTable<String, Integer, Integer> getRecordsPerPublYear(final Connection connection,
			final int lteYear) {

		final TreeBasedTable<String, Integer, Integer> recordsPerPublYear = TreeBasedTable.create();

		final String aggregateLogFormat = "{}: Aggregating records per publication year from view '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", PROCEEDINGS_RECORD_COUNT_VIEW);

		try (final PreparedStatement aggregateRecordsPerPublYear = connection
				.prepareStatement(getRecordsPerEventCmd(lteYear))) {

			final ResultSet recordsPerPublYearResults = aggregateRecordsPerPublYear.executeQuery();

			while (recordsPerPublYearResults.next()) {

				final String streamKey = recordsPerPublYearResults.getString(STREAM_KEY);
				final int publicationYear = recordsPerPublYearResults.getInt(PUBLICATION_YEAR);
				final int recordCount = recordsPerPublYearResults.getInt(RECORD_COUNT);

				recordsPerPublYear.put(streamKey, publicationYear, recordCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", PROCEEDINGS_RECORD_COUNT_VIEW), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", PROCEEDINGS_RECORD_COUNT_VIEW,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableTable.copyOf(recordsPerPublYear);
	}

	private static String getAuthorsPerEventCmd(final int lteYear) {

		final String authorsPerEventCmd = String.format(

				"select * " + "from %s " + ";",

				PROCEEDINGS_AUTHOR_COUNT_VIEW);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", authorsPerEventCmd);
		return authorsPerEventCmd;
	}

	private static String getRecordsPerEventCmd(final int lteYear) {

		final String recordsPerEventCmd = String.format(

				"select * " + "from %s " + ";",

				PROCEEDINGS_RECORD_COUNT_VIEW);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", recordsPerEventCmd);
		return recordsPerEventCmd;
	}

}
