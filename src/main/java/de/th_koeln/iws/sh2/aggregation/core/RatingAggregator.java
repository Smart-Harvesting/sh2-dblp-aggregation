package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_RANK;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_RATINGS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_RATING_SCORES;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.TreeBasedTable;
import com.google.common.collect.TreeMultimap;

/**
 * Aggregator for rating scores of conferences.
 * 
 * <p>
 * Depends on {@code CONFERENCE_RATINGS}.
 * 
 * <p>
 * Creates {@code RAW_RATING_SCORES}.
 * 
 * <p>
 * The rating score for each conference is calculated by averaging over the
 * rating score for each event. The rating score for each event is the average
 * over the numeric representation of the rating rank assigned by the local
 * ratings after mapping the rated entities to dblp conferences.
 *
 * @author michels
 *
 */
public class RatingAggregator extends CoreComponent {

	private static Logger LOGGER = LogManager.getLogger(RatingAggregator.class);
	private static final int BATCH_SIZE = 1000;

	/**
	 * Wrapper method. Reads the required data, performs the calculation and
	 * batch-inserts the results in a new table.
	 *
	 * @param connection the database connection
	 */
	public static void wrapTo(final Connection connection) {

		ImmutableTable<String, String, Integer> numericDblpRatings = getNumericDblpRatings(connection);

		reCreateTable(connection, RAW_RATING_SCORES, getCreateRawRatingScoresCmd());

		final String insertLogFormat = "{}: Batch-inserting into table '{}'";
		final Instant insertRatingsStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", RAW_RATING_SCORES);

		int streamCount = 0;

		try (final PreparedStatement insertAverageRatings = connection
				.prepareStatement(getInsertRawRatingScoresCmd())) {

			for (final String streamKey : numericDblpRatings.rowKeySet()) {

				LOGGER.debug("[{}] numeric ratings: {}", streamKey, numericDblpRatings.row(streamKey).values());

				final double rawRatingScore = numericDblpRatings.row(streamKey).values().stream()
						.collect(Collectors.summingDouble(Integer::doubleValue))
						/ numericDblpRatings.row(streamKey).values().size();

				LOGGER.debug("[{}] raw rating score: {}", streamKey, rawRatingScore);

				insertAverageRatings.setString(1, streamKey);
				insertAverageRatings.setDouble(2, rawRatingScore);

				insertAverageRatings.addBatch();
				if ((++streamCount % BATCH_SIZE) == 0)
					insertAverageRatings.executeBatch();
			}
			insertAverageRatings.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", RAW_RATING_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", RAW_RATING_SCORES,
				Duration.between(insertRatingsStart, Instant.now()));
	}

	/**
	 * Helper method to map the rating ranks to a numeric representation.
	 *
	 * @param connection the database connection
	 * 
	 * @return defensive copy of a tree-based table (keys = stream keys; column keys
	 *         = rating keys; cell values = mapped numeric rating ranks)
	 */
	private static ImmutableTable<String, String, Integer> getNumericDblpRatings(final Connection connection) {

		ImmutableTable<String, String, String> rawDblpRatings = getRawDblpRatings(connection);

		final TreeBasedTable<String, String, Integer> numericDblpRatings = TreeBasedTable.create();

		for (final Cell<String, String, String> cell : rawDblpRatings.cellSet()) {
			numericDblpRatings.put(cell.getRowKey(), cell.getColumnKey(),
					getAllRatingCategories(rawDblpRatings).indexOf(cell.getValue()) + 1);
		}
		return ImmutableTable.copyOf(numericDblpRatings);
	}

	/**
	 * Helper method to get the raw rating ranks for conferences.
	 *
	 * @param connection the database connection
	 * 
	 * @return defensive copy of a tree-based table (keys = stream keys; column keys
	 *         = rating keys; cell values = raw rating ranks)
	 */
	private static ImmutableTable<String, String, String> getRawDblpRatings(final Connection connection) {

		final ImmutableSetMultimap<String, String> duplicateRatingRegister = getDuplicateRatingRegister(connection);

		final TreeBasedTable<String, String, String> rawDblpRatings = TreeBasedTable.create();

		final String aggregateLogFormat = "{}: Aggregating from table '{}'";
		final Instant insertStreamsStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", CONFERENCE_RATINGS);

		try (final PreparedStatement selectAllRatings = connection.prepareStatement(getSelectAllRatingsCmd())) {

			final ResultSet ratings = selectAllRatings.executeQuery();

			while (ratings.next()) {

				final String ratingKey = ratings.getString(RATING_KEY);
				final String streamKey = ratings.getString(STREAM_KEY);

				if (!duplicateRatingRegister.containsEntry(ratingKey, streamKey)) {
					final String ratingRank = ratings.getString(RATING_RANK).replace("A*", "*A");
					rawDblpRatings.put(streamKey, ratingKey, ratingRank);
				}
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", CONFERENCE_RATINGS), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", CONFERENCE_RATINGS,
				Duration.between(insertStreamsStart, Instant.now()));
		return ImmutableTable.copyOf(rawDblpRatings);
	}

	/**
	 * Helper method to get a multimap of conferences which have been attributed
	 * with more than one rating rank by the same rating (either an entity
	 * resolution error on the side of the external rating or a mapping error on the
	 * side of dblp).
	 *
	 * @param connection the database connection
	 * 
	 * @return defensive copy of a multimap (keys = rating keys; values = stream
	 *         keys)
	 */
	private static ImmutableSetMultimap<String, String> getDuplicateRatingRegister(final Connection connection) {

		final SetMultimap<String, String> duplicateRatingRegister = TreeMultimap.create();

		final String registerLogFormat = "{}: Registering duplicates for table '{}'";
		final Instant registerDuplicatesStart = Instant.now();
		LOGGER.info(registerLogFormat, "START", CONFERENCE_RATINGS);

		try (final PreparedStatement selectDuplicateRatings = connection
				.prepareStatement(getSelectDuplicateRatingsCmd())) {

			final ResultSet duplicates = selectDuplicateRatings.executeQuery();

			while (duplicates.next()) {

				final String ratingKey = duplicates.getString(RATING_KEY);
				final String streamKey = duplicates.getString(STREAM_KEY);

				duplicateRatingRegister.put(ratingKey, streamKey);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(registerLogFormat, "FAILED", CONFERENCE_RATINGS), e);
			System.exit(1);
		}
		LOGGER.info(registerLogFormat + " (Duration: {})", "END", CONFERENCE_RATINGS,
				Duration.between(registerDuplicatesStart, Instant.now()));
		return ImmutableSetMultimap.copyOf(duplicateRatingRegister);
	}

	private static ImmutableList<String> getAllRatingCategories(ImmutableTable<String, String, String> rawDblpRatings) {
		return FluentIterable.from(rawDblpRatings.values()).toSortedSet(Comparator.reverseOrder()).asList();
	}

	@SuppressWarnings("unused")
	private static ImmutableSetMultimap<String, String> getDblpRatingMetadata(
			ImmutableTable<String, String, String> rawDblpRatings) {

		final SortedSetMultimap<String, String> dblpRatingMetadata = TreeMultimap.create();

		for (String columnKey : rawDblpRatings.columnKeySet()) {
			dblpRatingMetadata.putAll(columnKey, rawDblpRatings.column(columnKey).values());
		}
		return ImmutableSetMultimap.copyOf(dblpRatingMetadata);
	}

	private static String getSelectAllRatingsCmd() {
		return String.format(

				"select * from %s" + ";",

				CONFERENCE_RATINGS);
	}

	private static String getSelectDuplicateRatingsCmd() {
		return String.format(

				"select * from ("

						+ "select %s, %s, count(%s) "

						+ "from %s "

						+ "group by %s, %s" + ") sq "

						+ "where count > 1" + ";",

						STREAM_KEY, RATING_KEY, RATING_RANK,

						CONFERENCE_RATINGS,

						STREAM_KEY, RATING_KEY);
	}

	private static String getCreateRawRatingScoresCmd() {
		return String.format(

				"create table if not exists %s " + "( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s numeric not null " + ")" + ";",

						RAW_RATING_SCORES,

						GENERIC_ID,

						STREAM_KEY, RATING_SCORE);
	}

	private static String getInsertRawRatingScoresCmd() {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

						RAW_RATING_SCORES,

						STREAM_KEY, RATING_SCORE);
	}
}
