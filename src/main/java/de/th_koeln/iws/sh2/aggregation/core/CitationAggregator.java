package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITATION_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITING_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITING_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.INCOMING_CITATIONS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_CITATION_SCORES;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeBasedTable;

/**
 * Aggregator for citation scores of conferences.
 * 
 * <p>
 * Depends on {@code INCOMING_CITATIONS}.
 * 
 * <p>
 * Creates {@code RAW_CITATION_SCORES}.
 * 
 * <p>
 * The citation score for each conference is calculated by averaging over the
 * citation score for each event. The citation score for each event is the
 * normalized weighted average over the citation score for each
 * {@code CITING_YEAR}. The normalized weights decline over the
 * {@code CITING_YEAR}s. The citation score for a {@code CITING_YEAR} of an
 * event equals the citation count of the event in the current
 * {@code CITING_YEAR} divided by the total of citing papers mapped to dblp in
 * that year and by the number of records of the event.
 *
 * @author michels
 *
 */
public class CitationAggregator extends CoreAggregator {

	private static final Logger LOGGER = LogManager.getLogger(CitationAggregator.class);
	private static final int BATCH_SIZE = 1000;

	/**
	 * local constant used to describe a count column in an aggregating
	 * SQL-expression returned by @see
	 * CitationAggregator#getGroupByStreamTargetCitingCmd(int)
	 */
	private static final String INCOMING_CITE_COUNT = "incoming_cite_count";

	/**
	 * Wrapper method. Reads the required data, performs the calculation and
	 * batch-inserts the results in a new table.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 */
	public static void wrapTo(final Connection connection, final int lteYear) throws SQLException {

		final ImmutableSortedMap<String, Double> rawCitationScores = computeCitationScores(connection, lteYear);

		reCreateTable(connection, RAW_CITATION_SCORES, getCreateCitationScoresCmd());

		final String insertLogFormat = "{}: Batch-inserting raw citation scores into table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", RAW_CITATION_SCORES);

		try (final PreparedStatement insertCitationScores = connection.prepareStatement(getInsertCitationScoresCmd())) {

			int streamCount = 0;

			for (Entry<String, Double> entry : rawCitationScores.entrySet()) {

				insertCitationScores.setString(1, entry.getKey());
				insertCitationScores.setDouble(2, entry.getValue());

				insertCitationScores.addBatch();

				if ((++streamCount % BATCH_SIZE) == 0) {
					LOGGER.info("Processed {} streams...", streamCount);
					insertCitationScores.executeBatch();
				}
			}
			insertCitationScores.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", RAW_CITATION_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", RAW_CITATION_SCORES,
				Duration.between(insertStart, Instant.now()));
	}

	/**
	 * Helper method to compute the citation scores of each conference stream.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a sorted map (keys = {@code STREAM_KEY}s; values =
	 *         citation scores)
	 */
	private static ImmutableSortedMap<String, Double> computeCitationScores(final Connection connection,
			final int lteYear) {

		final ImmutableSortedMap<String, ImmutableTable<String, Integer, Integer>> incomingCitations =

				aggregateIncomingCitations(connection, lteYear);

		final ImmutableTable<String, Integer, Integer> citingYearStreamTotals =

				getCitingYearStreamTotals(incomingCitations);

		final ImmutableSortedMap<String, ImmutableTable<String, Integer, Double>> normalizedWeights =

				getNormalizedWeights(incomingCitations);

		final ImmutableTable<String, String, Integer> recordsPerCite =

				getRecordsPerCite(connection, lteYear);

		final SortedMap<String, Double> citationScores = new TreeMap<>();

		for (String streamKey : normalizedWeights.keySet()) {

			final List<Double> citeCitationScores = Lists.newArrayList();

			for (String citeKey : normalizedWeights.get(streamKey).rowKeySet()) {

				if (recordsPerCite.get(streamKey, citeKey) != null) {

					final int citeRecordCount = recordsPerCite.get(streamKey, citeKey);
					LOGGER.debug("[{}, {}, {}] cite record count: {}", lteYear, streamKey, citeKey, citeRecordCount);

					final List<Double> yearCitationScores = Lists.newArrayList();

					for (int citingYear : normalizedWeights.get(streamKey).row(citeKey).keySet()) {

						final int citationCount = incomingCitations.get(streamKey).get(citeKey, citingYear);
						final int citingYearTotal = citingYearStreamTotals.column(citingYear).values().stream()
								.collect(Collectors.summingInt(Integer::intValue));
						final double normalizedWeight = normalizedWeights.get(streamKey).get(citeKey, citingYear);

						LOGGER.debug("[{}, {}, {}, {}] citation count: {}", lteYear, streamKey, citeKey, citingYear,
								citationCount);
						LOGGER.debug("[{}, {}, {}] citing-year citation total: {}", lteYear, streamKey, citingYear,
								citingYearTotal);
						LOGGER.debug("[{}, {}, {}, {}] normalized weight: {}", lteYear, streamKey, citeKey, citingYear,
								normalizedWeight);

						final double yearCitationScore = ((1.0 * citationCount) / citeRecordCount / citingYearTotal)
								* normalizedWeight;
						LOGGER.debug("[{}, {}, {}, {}] citing-year raw citation score: {}", lteYear, streamKey, citeKey,
								citingYear, yearCitationScore);

						yearCitationScores.add(yearCitationScore);
					}

					LOGGER.debug("[{}, {}, {}] cites' year citation score: {}", lteYear, streamKey, citeKey,
							yearCitationScores);

					citeCitationScores
					.add(yearCitationScores.stream().collect(Collectors.summingDouble(Double::doubleValue)));

				} else {
					LOGGER.error("Missing coordinates in 'recordsPerCite': (" + streamKey + ", " + citeKey + ")");
				}
			}
			LOGGER.debug("[{}, {}] cites' raw citation scores: {}", lteYear, streamKey, citeCitationScores);

			final double rawCitationScore = citeCitationScores.stream()
					.collect(Collectors.summingDouble(Double::doubleValue)) / citeCitationScores.size();

			if (Double.isFinite(rawCitationScore)) {
				LOGGER.debug("[{}, {}] raw citation score: {}", lteYear, streamKey, rawCitationScore);
				citationScores.put(streamKey, rawCitationScore);
			}

		}
		return ImmutableSortedMap.copyOf(citationScores);
	}

	/**
	 * Helper method to compute the number of incoming citations for each
	 * {@code CITING_YEAR} of each conference event.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a sorted map (keys = {@code STREAM_KEY}s; values =
	 *         defensive copies of tree-based tables (row keys = {@code CITE_KEY}s
	 *         of the target event; column keys = {@code CITING_YEAR}s; cell values
	 *         = citation counts))
	 */
	private static ImmutableSortedMap<String, ImmutableTable<String, Integer, Integer>> aggregateIncomingCitations(
			final Connection connection, final int lteYear) {

		final String incomingCitationsLogFormat = "{}: Aggregating incoming citations from table '{}'";
		final Instant incomingCitationsStart = Instant.now();
		LOGGER.info(incomingCitationsLogFormat, "START", INCOMING_CITATIONS);

		final SortedMap<String, TreeBasedTable<String, Integer, Integer>> incomingCitations = new TreeMap<>();

		try (final PreparedStatement aggregateIncomingCitations = connection
				.prepareStatement(getAggregateIncomingCitationsCmd(lteYear))) {

			final ResultSet incomingCitationsResults = aggregateIncomingCitations.executeQuery();

			while (incomingCitationsResults.next()) {
				final String streamKey = incomingCitationsResults.getString(STREAM_KEY);
				if (!incomingCitations.containsKey(streamKey)) {
					incomingCitations.put(streamKey, TreeBasedTable.create());
				}
				incomingCitations.get(streamKey).put(incomingCitationsResults.getString(CITE_KEY),
						incomingCitationsResults.getInt(CITING_YEAR),
						incomingCitationsResults.getInt(INCOMING_CITE_COUNT));
			}
		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(incomingCitationsLogFormat, "FAILED", INCOMING_CITATIONS), e);
			System.exit(1);
		}
		LOGGER.info(incomingCitationsLogFormat + " (Duration: {})", "END", INCOMING_CITATIONS,
				Duration.between(incomingCitationsStart, Instant.now()));
		return ImmutableSortedMap.copyOf(incomingCitations.entrySet().stream()
				.map(e -> Maps.immutableEntry(e.getKey(), ImmutableTable.copyOf(e.getValue())))
				.collect(Collectors.toSet()));
	}

	/**
	 * Helper method to compute the total of incoming citations for each
	 * {@code CITING_YEAR} for each conference.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 *                   // * @return defensive copy of a tree-based table (row keys
	 *                   = {@code STREAM_KEY}s; column keys = {@code CITING_YEAR}s;
	 *                   cell values = total citation counts)
	 */
	private static ImmutableTable<String, Integer, Integer> getCitingYearStreamTotals(
			final ImmutableSortedMap<String, ImmutableTable<String, Integer, Integer>> incomingCitations) {

		final TreeBasedTable<String, Integer, Integer> citingYearStreamTotals = TreeBasedTable.create();

		for (String streamKey : incomingCitations.keySet()) {

			for (int citingYear : incomingCitations.get(streamKey).columnKeySet()) {

				final int citingYearStreamTotal = incomingCitations.get(streamKey).column(citingYear).values().stream()
						.collect(Collectors.summingInt(Integer::intValue));

				citingYearStreamTotals.put(streamKey, citingYear, citingYearStreamTotal);
			}
		}
		return ImmutableTable.copyOf(citingYearStreamTotals);
	}

	/**
	 * Helper method to compute the normalized weights for each {@code CITING_YEAR}
	 * of each conference event.
	 *
	 * @param connection        the database connection
	 * @param incomingCitations a map of the number of incoming citations for each
	 *                          {@code CITING_YEAR} of each conference event (as
	 *                          returned by @see
	 *                          {@link CitationAggregator#aggregateIncomingCitations(Connection, int)}
	 *                          ).
	 * 
	 * @return defensive copy of a sorted map (keys = {@code STREAM_KEY}s; values =
	 *         defensive copies of tree-based tables (row keys = {@code CITE_KEY}s
	 *         of the target event; column keys = {@code CITING_YEAR}s; cell values
	 *         = normalized weights))
	 */
	private static ImmutableSortedMap<String, ImmutableTable<String, Integer, Double>> getNormalizedWeights(
			final ImmutableSortedMap<String, ImmutableTable<String, Integer, Integer>> incomingCitations) {

		final SortedMap<String, TreeBasedTable<String, Integer, Double>> normalizedWeights = new TreeMap<>();

		for (String streamKey : incomingCitations.keySet()) {
			for (String citeKey : incomingCitations.get(streamKey).rowKeySet()) {

				ImmutableList<Integer> eventCitingYears = incomingCitations.get(streamKey).row(citeKey).keySet()
						.stream().sorted().collect(ImmutableList.toImmutableList());
				ImmutableList<Double> normalizedEventWeights = getNormalizedEventWeights(eventCitingYears);

				for (int citingYear : eventCitingYears) {
					if (!normalizedWeights.containsKey(streamKey)) {
						normalizedWeights.put(streamKey, TreeBasedTable.create());
					}
					normalizedWeights.get(streamKey).put(citeKey, citingYear,
							normalizedEventWeights.get(eventCitingYears.indexOf(citingYear)));
				}
			}
		}
		return ImmutableSortedMap.copyOf(normalizedWeights.entrySet().stream()
				.map(e -> Maps.immutableEntry(e.getKey(), ImmutableTable.copyOf(e.getValue())))
				.collect(Collectors.toSet()));
	}

	/**
	 * Helper method to compute the normalized weights for each {@code CITING_YEAR}
	 * existing for a given conference event.
	 *
	 * @param eventCitingYears the list of {@code CITING_YEAR}s existing for a given
	 *                         conference event
	 * 
	 * @return defensive copy of the list of normalized weights for each
	 *         {@code CITING_YEAR} in the given list
	 */
	private static ImmutableList<Double> getNormalizedEventWeights(final ImmutableList<Integer> eventCitingYears) {
		return eventCitingYears.stream()
				.map(citingYear -> ((eventCitingYears.size() - eventCitingYears.indexOf(citingYear)) * 1.0)
						/ getWeightDenominator(eventCitingYears.size()))
				.collect(ImmutableList.toImmutableList());
	}

	/**
	 * Helper method to compute the dominator necessary to normalize a given number
	 * of weights.
	 *
	 * @param weightCount the number of weights required
	 * 
	 * @return the denominator to normalize the given number of weights
	 */
	private static int getWeightDenominator(final int weightCount) {
		int denominator = 0;
		for (int i = 1; i <= weightCount; i++) {
			denominator += i;
		}
		return denominator;
	}

	private static String getAggregateIncomingCitationsCmd(final int lteYear) {

		final String aggregateIncomingCiteCountCmd = String.format(

				"select %s, %s, %s, "

						+ "count(%s) as %s "

						+ "from %s "

						+ "where %s > 0 and %s < 3333 "

						+ "group by %s, %s, %s" + ";",

						STREAM_KEY, CITE_KEY, CITING_YEAR,

						CITING_KEY, INCOMING_CITE_COUNT,

						INCOMING_CITATIONS,

						CITING_YEAR, CITING_YEAR,

						STREAM_KEY, CITE_KEY, CITING_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", aggregateIncomingCiteCountCmd);
		return aggregateIncomingCiteCountCmd;
	}

	private static String getCreateCitationScoresCmd() {

		final String createCitationScoresCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s numeric not null " + ")" + ";",

						RAW_CITATION_SCORES,

						GENERIC_ID,

						STREAM_KEY, CITATION_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createCitationScoresCmd);
		return createCitationScoresCmd;
	}

	private static String getInsertCitationScoresCmd() {

		final String insertCitationScoresCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

						RAW_CITATION_SCORES,

						STREAM_KEY, CITATION_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertCitationScoresCmd);
		return insertCitationScoresCmd;
	}

}
