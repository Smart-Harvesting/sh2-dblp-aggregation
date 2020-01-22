package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PERSON_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PROMINENCE_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_COUNT;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_PROMINENCE_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.DETAIL_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PERSON_YEAR_RECORD_COUNT_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_VIEW;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.TreeBasedTable;

/**
 * Aggregator for prominence scores of conferences.
 * 
 * <p>
 * Depends on {@code TOC_VIEW}, {@code DETAIL_VIEW},
 * {@code RECORDS_PER_EVENT_VIEW}, {@code RECORD_COUNT_VIEW}.
 * 
 * <p>
 * Creates {@ref RAW_PROMINENCE_SCORES}.
 * 
 * <p>
 * The prominence score for each conference is calculated by averaging over the
 * prominence score for each event. The prominence score of an event equals the
 * number of records of its distinct authors divided by the number of its
 * distinct authors.
 *
 * @author michels
 *
 */
public class ProminenceAggregator extends CoreAggregator {

	private static final Logger LOGGER = LogManager.getLogger(ProminenceAggregator.class);
	private static final int BATCH_SIZE = 1000;

	// TODO: Move to ColumnNames?
	private static String getRecordCountLteCol(int lteYear) {
		return String.format("record_count_lte_%d", lteYear);
	}

	/**
	 * Wrapper method. Reads the required data, performs the calculation and
	 * batch-inserts the results in a new table.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 */
	public static void wrapTo(final Connection connection, final int lteYear) throws SQLException {

		ImmutableTable<String, String, Double> rawProminenceScores = computeProminenceScores(connection, lteYear);

		reCreateTable(connection, RAW_PROMINENCE_SCORES, getCreateProminenceScoresCmd());

		final String insertLogFormat = "{}: Batch-inserting raw prominence scores into table '{}'";
		final Instant insertProminenceStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", RAW_PROMINENCE_SCORES);

		int streamCount = 0;

		try (final PreparedStatement insertProminenceScore = connection
				.prepareStatement(getInsertProminenceScoresCmd())) {

			for (String streamKey : rawProminenceScores.rowKeySet()) {

				final double rawProminenceScore = rawProminenceScores.row(streamKey).values().stream()
						.collect(Collectors.summingDouble(Double::doubleValue))
						/ rawProminenceScores.row(streamKey).values().size();

				LOGGER.debug("[{}, {}] raw prominence score: {}", lteYear, streamKey, rawProminenceScore);

				insertProminenceScore.setString(1, streamKey);
				insertProminenceScore.setDouble(2, rawProminenceScore);

				insertProminenceScore.addBatch();

				if ((++streamCount % BATCH_SIZE) == 0) {
					LOGGER.info("Processed {} streams...", streamCount);
					insertProminenceScore.executeBatch();
				}
			}
			insertProminenceScore.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", RAW_PROMINENCE_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", RAW_PROMINENCE_SCORES,
				Duration.between(insertProminenceStart, Instant.now()));
	}

	/**
	 * Helper method to compute the prominence scores of each conference event.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code CITE_KEY}s; cell values = prominence scores)
	 */
	private static ImmutableTable<String, String, Double> computeProminenceScores(final Connection connection,
			final int lteYear) {

		final ImmutableTable<String, String, Integer> authorsPerCite = getAuthorsPerCite(connection, lteYear);
		final ImmutableTable<String, String, Integer> authorsRecordCountsPerCite = aggregateAuthorsRecordCountsPerCite(
				connection, lteYear);

		final TreeBasedTable<String, String, Double> prominenceScores = TreeBasedTable.create();

		for (String streamKey : authorsRecordCountsPerCite.rowKeySet()) {
			for (String citeKey : authorsRecordCountsPerCite.columnKeySet()) {
				if ((authorsRecordCountsPerCite.get(streamKey, citeKey) != null)
						&& (authorsPerCite.get(streamKey, citeKey) != null)) {

					final int authorsRecordCount = authorsRecordCountsPerCite.get(streamKey, citeKey);

					LOGGER.debug("[{}, {}, {}] authors' record count: {}", lteYear, streamKey, citeKey,
							authorsRecordCount);
					LOGGER.debug("[{}, {}, {}] cite author count: {}", lteYear, streamKey, citeKey, authorsRecordCount);

					final int citeAuthorsCount = authorsPerCite.get(streamKey, citeKey);

					LOGGER.debug("[{}, {}, {}] raw cite prominence score: {}", lteYear, streamKey, citeKey,
							(1.0 * authorsRecordCount) / citeAuthorsCount);

					prominenceScores.put(streamKey, citeKey, (1.0 * authorsRecordCount) / citeAuthorsCount);

				} else if ((authorsRecordCountsPerCite.get(streamKey, citeKey) != null)
						&& (authorsPerCite.get(streamKey, citeKey) == null)) {
					LOGGER.error("Missing coordinates in 'authorsPerCite': (" + streamKey + ", " + citeKey + ")");
				}

			}
		}
		return ImmutableTable.copyOf(prominenceScores);

	}

	/**
	 * Helper method to get the number of records published by all distinct authors
	 * of an event.
	 *
	 * @param connection the database connection
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code PUBLICATION_YEAR}s; cell values = number of
	 *         records published by all distinct authors)
	 */
	private static ImmutableTable<String, String, Integer> aggregateAuthorsRecordCountsPerCite(
			final Connection connection, final int lteYear) {

		final TreeBasedTable<String, String, Integer> authorsRecordCountsPerCite = TreeBasedTable.create();

		final String aggregateFormat = "{}: Aggregating authors' record count per cite";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateFormat, "START");

		try (final PreparedStatement aggregateAuthorsRecordCounts = connection
				.prepareStatement(getAggregateAuthorsRecordCountsCmd(lteYear))) {

			final ResultSet authorsRecordCountsResults = aggregateAuthorsRecordCounts.executeQuery();

			while (authorsRecordCountsResults.next()) {

				final String streamKey = authorsRecordCountsResults.getString(STREAM_KEY);
				final String citeKey = authorsRecordCountsResults.getString(CITE_KEY);
				final int recordCount = authorsRecordCountsResults.getInt(getRecordCountLteCol(lteYear));

				authorsRecordCountsPerCite.put(streamKey, citeKey, recordCount);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateFormat, "FAILED"), e);
			System.exit(1);
		}
		LOGGER.info(aggregateFormat + " (Duration: {})", "END", Duration.between(aggregateStart, Instant.now()));
		return ImmutableTable.copyOf(authorsRecordCountsPerCite);
	}

	private static String getAggregateAuthorsRecordCountsCmd(final int lteYear) {

		final String innerSelectDistinctEventAuthors = String.format(

				"select distinct %s, %s, %s "

						+ "from %s "

						+ "where %s <= '%d-12-31'",

						STREAM_KEY, CITE_KEY, PERSON_KEY,

						DETAIL_VIEW,

						C_DATE, lteYear);

		final String innerSelectAuthorYearRecordCount = String.format(

				"select %s, sum(%s) %s "

						+ "from %s "

						+ "group by %s",

						PERSON_KEY, RECORD_COUNT, getRecordCountLteCol(lteYear),

						PERSON_YEAR_RECORD_COUNT_VIEW,

						PERSON_KEY);

		final String joinEventAuthorsAndCounts = String.format(

				"select %s, %s, sum(%s) %s "

						+ "from (%s) dea join (%s) arc "

						+ "using (%s) "

						+ "group by %s, %s",

						STREAM_KEY, CITE_KEY, getRecordCountLteCol(lteYear), getRecordCountLteCol(lteYear),

						innerSelectDistinctEventAuthors, innerSelectAuthorYearRecordCount,

						PERSON_KEY,

						STREAM_KEY, CITE_KEY);

		final String aggregateAuthorsRecordCountsCmd = String.format(

				"select * from (%s) aggregated "

						+ "left join %s "

						+ "using (%s, %s) "

						+ ";",

						joinEventAuthorsAndCounts,

						PROCEEDINGS_VIEW,

						STREAM_KEY, CITE_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", aggregateAuthorsRecordCountsCmd);
		return aggregateAuthorsRecordCountsCmd;

	}

	private static String getCreateProminenceScoresCmd() {

		final String createProminenceScoresCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s numeric not null " + ")" + ";",

						RAW_PROMINENCE_SCORES,

						GENERIC_ID,

						STREAM_KEY, PROMINENCE_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createProminenceScoresCmd);
		return createProminenceScoresCmd;
	}

	private static String getInsertProminenceScoresCmd() {

		final String insertProminenceScoresCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

						RAW_PROMINENCE_SCORES,

						STREAM_KEY, PROMINENCE_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertProminenceScoresCmd);
		return insertProminenceScoresCmd;
	}

}
