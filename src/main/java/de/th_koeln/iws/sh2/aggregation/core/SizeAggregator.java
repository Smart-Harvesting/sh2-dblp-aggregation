package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SIZE_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_SIZE_SCORES;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableTable;

public class SizeAggregator extends CoreAggregator {

	private static Logger LOGGER = LogManager.getLogger(SizeAggregator.class);
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

		final ImmutableSortedMap<String, Double> rawSizeScores = computeSizeScores(connection, lteYear);

		reCreateTable(connection, RAW_SIZE_SCORES, getCreateSizeScoresCmd());

		final String insertLogFormat = "{}: Batch-inserting raw size scores into table '{}'";
		final Instant insertScoresStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", RAW_SIZE_SCORES);

		try (final PreparedStatement insertSizeScores = connection.prepareStatement(getInsertSizeScoresCmd())) {

			int streamCount = 0;

			for (Entry<String, Double> entry : rawSizeScores.entrySet()) {

				insertSizeScores.setString(1, entry.getKey());
				insertSizeScores.setDouble(2, entry.getValue());

				insertSizeScores.addBatch();

				if ((++streamCount % BATCH_SIZE) == 0) {
					LOGGER.info("Processed {} streams...", streamCount);
					insertSizeScores.executeBatch();
				}
			}
			insertSizeScores.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", RAW_SIZE_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", RAW_SIZE_SCORES,
				Duration.between(insertScoresStart, Instant.now()));
	}

	/**
	 * Helper method to compute the size scores of each conference stream.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a sorted map (keys = {@code STREAM_KEY}s; values =
	 *         size scores)
	 */
	private static ImmutableSortedMap<String, Double> computeSizeScores(final Connection connection,
			final int lteYear) {

		final ImmutableTable<String, String, Integer> recordsPerCite = getRecordsPerCite(connection, lteYear);

		final SortedMap<String, Double> sizeScores = new TreeMap<>();

		for (String streamKey : recordsPerCite.rowKeySet()) {

			final int rowSum = recordsPerCite.row(streamKey).values().stream()
					.collect(Collectors.summingInt(Integer::intValue));
			final int rowCount = recordsPerCite.row(streamKey).values().size();
			final double rawSizeScore = (rowSum * 1.0) / rowCount;

			LOGGER.debug("[{}, {}] papers of all cites: {}", lteYear, streamKey, rowSum);
			LOGGER.debug("[{}, {}] cite count: {}", lteYear, streamKey, rowCount);
			LOGGER.debug("[{}, {}] raw size score: {}", lteYear, streamKey, rawSizeScore);

			sizeScores.put(streamKey, rawSizeScore);
		}
		return ImmutableSortedMap.copyOf(sizeScores);
	}

	private static String getCreateSizeScoresCmd() {
		final String createSizeScoresCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s numeric not null " + ")" + ";",

						RAW_SIZE_SCORES,

						GENERIC_ID,

						STREAM_KEY, SIZE_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createSizeScoresCmd);
		return createSizeScoresCmd;
	}

	private static String getInsertSizeScoresCmd() {
		final String insertSizeScoresCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

						RAW_SIZE_SCORES, STREAM_KEY, SIZE_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertSizeScoresCmd);
		return insertSizeScoresCmd;
	}
}
