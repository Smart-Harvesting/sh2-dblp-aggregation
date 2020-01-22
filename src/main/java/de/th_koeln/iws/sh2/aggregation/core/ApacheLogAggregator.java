package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.LOG_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_CIRCLES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_SWAPS;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_TIME_PER_PAGE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_TOTAL_PAGES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_TOTAL_TIME;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSION_STRUCTURE_LOG_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSION_STRUCTURE_SESSION_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_LOG_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SESSIONS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SESSION_STRUCTURE;
import static org.sh2.ala.core.util.ColumnNames.CLIENT_HOST;
import static org.sh2.ala.core.util.ColumnNames.GENERIC_TIMESTAMP;
import static org.sh2.ala.core.util.ColumnNames.GET_REQUEST;
import static org.sh2.ala.core.util.ColumnNames.REFERER;
import static org.sh2.ala.core.util.ColumnNames.USER_AGENT;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;

public class ApacheLogAggregator extends CoreAggregator {

	private static final int TOTAL_TIME_THRESHOLD = 196;
	private static final int TOTAL_PAGES_THRESHOLD = 9;
	private static final int TIME_PER_PAGE_THRESHOLD = 24;

	private static final int CIRCLE_THRESHOLD = 4;
	private static final int SWAP_THRESHOLD = 1;

	private static Logger LOGGER = LogManager.getLogger(ApacheLogAggregator.class);
	private static final int BATCH_SIZE = 1000;

	/**
	 * Wrapper method. Reads the required data, performs the calculation and
	 * batch-inserts the results in a new table.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * @throws SQLException
	 */
	public static void wrapTo(final Connection connection, final String logsTableName) throws SQLException {

		final ImmutableSortedSet<String> logTablePartSuffixes = getLogTablePartSuffixes(connection, logsTableName);
		final ImmutableSortedSet<String> streamKeys = getStreamKeys(connection);

		reCreateTable(connection, RAW_LOG_SCORES, getCreateLogScoresCmd());

		final String insertLogFormat = "{}: Batch-inserting stream keys in table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", RAW_LOG_SCORES);

		try (final PreparedStatement insertStreamKey = connection.prepareStatement(getInsertStreamKeyCmd())) {
			for (final String streamKey : streamKeys) {
				insertStreamKey.setString(1, streamKey);
				insertStreamKey.addBatch();
			}
			insertStreamKey.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", RAW_LOG_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", RAW_LOG_SCORES,
				Duration.between(insertStart, Instant.now()));

		for (final String logsPartitionSuffix : logTablePartSuffixes) {

			final ImmutableSortedMap<String, Double> rawLogScores = computeLogScores(connection, logsTableName,
					logsPartitionSuffix, streamKeys);

			final String updateLogFormat = "{}: Batch-updating raw log scores in table '{}'";
			final Instant updateStart = Instant.now();
			LOGGER.info(updateLogFormat, "START", RAW_LOG_SCORES);

			try (final PreparedStatement addLogScoreColumn = connection
					.prepareStatement(getAddLogScoreColumnCmd(logsPartitionSuffix));
					final PreparedStatement updateLogScores = connection
							.prepareStatement(getUpdateLogScoreCmd(logsPartitionSuffix))) {

				addLogScoreColumn.executeUpdate();

				int streamCount = 0;

				for (Entry<String, Double> entry : rawLogScores.entrySet()) {

					updateLogScores.setDouble(1, entry.getValue());
					updateLogScores.setString(2, entry.getKey());

					updateLogScores.addBatch();

					if ((++streamCount % BATCH_SIZE) == 0) {
						LOGGER.info("Processed {} streams...", streamCount);
						updateLogScores.executeBatch();
					}
				}
				updateLogScores.executeBatch();

			} catch (SQLException e) {
				LOGGER.fatal(new ParameterizedMessage(updateLogFormat, "FAILED", RAW_LOG_SCORES), e);
				System.exit(1);
			}
			LOGGER.info(updateLogFormat + " (Duration: {})", "END", RAW_LOG_SCORES,
					Duration.between(updateStart, Instant.now()));
		}
	}

	private static String getInsertStreamKeyCmd() {

		final String insertStreamKeyCmd = String.format(

				"insert into %s "

						+ "( " + "%s" + " ) "

						+ "values (?)" + ";",

						RAW_LOG_SCORES, STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertStreamKeyCmd);
		return insertStreamKeyCmd;
	}

	private static ImmutableSortedSet<String> getLogTablePartSuffixes(final Connection connection,
			final String logsTableName) {

		final Set<String> logTablePartSuffixes = Sets.newTreeSet();

		final String aggregateLogFormat = "{}: Aggregating partition names for table '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", logsTableName);

		try (final PreparedStatement getLogTablePartSuffixes = connection
				.prepareStatement(getLogTablePartSuffixesCmd(logsTableName))) {
			final ResultSet logTablePartSuffixesResults = getLogTablePartSuffixes.executeQuery();

			while (logTablePartSuffixesResults.next()) {
				final String partitionName = logTablePartSuffixesResults.getString(4);
				logTablePartSuffixes.add(partitionName.substring(partitionName.lastIndexOf("_") + 1));
			}
		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", logsTableName), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", logsTableName,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableSortedSet.copyOf(logTablePartSuffixes);
	}

	private static ImmutableSortedMap<String, Double> computeLogScores(final Connection connection,
			final String logsTableName, final String logsPartitionSuffix, final ImmutableSortedSet<String> streamKeys) {

		final Multiset<String> streamOccurrences = TreeMultiset.create();
		final Multiset<Long> sessionOccurrences = TreeMultiset.create();
		final TreeMultimap<String, Long> streamsToSessions = TreeMultimap.create();

		final String aggregateLogFormat = "{}: Aggregating log scores from logs '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", SESSIONS);

		try (final PreparedStatement aggregateStrugglingSessionLogs = connection
				.prepareStatement(getSelectStrugglingSessionLogsCmdCmd(logsTableName, logsPartitionSuffix))) {

			connection.setAutoCommit(false);
			aggregateStrugglingSessionLogs.setFetchSize(100000);

			final ResultSet strugglingSessionLogsResults = aggregateStrugglingSessionLogs.executeQuery();

			while (strugglingSessionLogsResults.next()) {

				final String getRequest = strugglingSessionLogsResults.getString(GET_REQUEST);
				final Long sessionId = strugglingSessionLogsResults.getLong(SESSION_STRUCTURE_SESSION_ID);

				sessionOccurrences.add(sessionId);

				if (streamKeys.parallelStream().anyMatch(getRequest::contains)) {

					Pattern pattern = Pattern.compile("^.*? /db/(.*)(/|index(\\.html)?) HTTP.*$");
					Matcher matcher = pattern.matcher(getRequest);

					if (matcher.find()) {
						final String streamKey = matcher.group(1).replaceAll("/$", "");

						LOGGER.debug("[{}, {}] matched get request: {}", logsPartitionSuffix, streamKey, getRequest);

						streamOccurrences.add(streamKey);
						streamsToSessions.put(streamKey, sessionId);
					}
				}
			}
			connection.commit();
			connection.setAutoCommit(true);

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", SESSIONS), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", SESSIONS,
				Duration.between(aggregateStart, Instant.now()));

		final TreeMap<String, Double> logScores = new TreeMap<>();

		// total number of struggling sessions which contain >= 1 conference streamKey
		long strugglingSessionsWithStreamCount = streamsToSessions.values().stream().distinct().count();

		LOGGER.debug("[{}] struggle(C): {}", logsPartitionSuffix, strugglingSessionsWithStreamCount);

		for (String streamKey : streamsToSessions.keySet()) {

			final double rawLogScore = (streamsToSessions.get(streamKey).size() * 1.0)
					/ strugglingSessionsWithStreamCount;

			LOGGER.debug("[{}, {}] struggle(c): {}", logsPartitionSuffix, streamKey,
					streamsToSessions.get(streamKey).size());
			LOGGER.debug("[{}, {}] raw log score: {}", logsPartitionSuffix, streamKey, rawLogScore);

			logScores.put(streamKey, rawLogScore);
		}

		return ImmutableSortedMap.copyOf(logScores);
	}

	private static String getSelectStrugglingSessionLogsCmdCmd(final String logsTableName,
			final String logsPartitionSuffix) {

		final String innerSelectStruggleCmd = String.format(

				"select %s from %s_%s as struggle "

						+ "where %s > %d "

						+ "and %s > %d "

						+ "and %s > %d "

						+ "and %s > %d "

						+ "and %s > %d",

						GENERIC_ID, SESSIONS, logsPartitionSuffix,

						SESSIONS_TOTAL_TIME, TOTAL_TIME_THRESHOLD,

						SESSIONS_TOTAL_PAGES, TOTAL_PAGES_THRESHOLD,

						SESSIONS_TIME_PER_PAGE, TIME_PER_PAGE_THRESHOLD,

						SESSIONS_CIRCLES, CIRCLE_THRESHOLD,

						SESSIONS_SWAPS, SWAP_THRESHOLD);

		final String innerSelectJoinWithSessionStructure = String.format(

				"select distinct %s, %s "

						+ "from %s_%s "

						+ "where %s in (%s)",

						SESSION_STRUCTURE_SESSION_ID, SESSION_STRUCTURE_LOG_ID,

						SESSION_STRUCTURE, logsPartitionSuffix,

						SESSION_STRUCTURE_SESSION_ID, innerSelectStruggleCmd);

		final String selectStrugglingSessionLogsCmd = String.format(

				"select struggle.*, "

						+ "logs.%s, " + "logs.%s, "

						+ "logs.%s, " + "logs.%s, "

						+ "logs.%s, " + "logs.%s "

						+ "from %s_%s logs "

						+ "join (%s) struggle " + "on logs.%s = struggle.%s "

						+ "order by struggle.%s, logs.%s, logs.%s",

						GENERIC_DATE, GENERIC_TIMESTAMP,

						GET_REQUEST, REFERER,

						USER_AGENT, CLIENT_HOST,

						logsTableName, logsPartitionSuffix,

						innerSelectJoinWithSessionStructure, GENERIC_ID, SESSION_STRUCTURE_LOG_ID,

						SESSION_STRUCTURE_SESSION_ID, GENERIC_ID, GENERIC_TIMESTAMP);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectStrugglingSessionLogsCmd);
		return selectStrugglingSessionLogsCmd;
	}

	private static String getLogTablePartSuffixesCmd(final String logsTableName) {
		final String logTablePartSuffixesCmd = String.format(

				"select " + "nmsp_parent.nspname as parent_schema" + ", " + "parent.relname as parent" + ", "
						+ "nmsp_child.nspname as child_schema" + ", " + "child.relname as child " + "from pg_inherits "
						+ "join pg_class parent on pg_inherits.inhparent = parent.oid "
						+ "join pg_class child on pg_inherits.inhrelid = child.oid "
						+ "join pg_namespace nmsp_parent on nmsp_parent.oid = parent.relnamespace "
						+ "join pg_namespace nmsp_child on nmsp_child.oid = child.relnamespace "
						+ "where parent.relname = '%s';",
						logsTableName);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", logTablePartSuffixesCmd);
		return logTablePartSuffixesCmd;
	}

	private static String getCreateLogScoresCmd() {
		final String createLogScoresCmd = String.format(

				"create table if not exists %s " + "( "

						+ "%s bigserial primary key" + ", "

						+ "%s varchar(255) not null" + " )" + ";",

						RAW_LOG_SCORES,

						GENERIC_ID,

						STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createLogScoresCmd);
		return createLogScoresCmd;
	}

	private static String getAddLogScoreColumnCmd(final String partSuffix) {
		final String addLogScoreColumnCmd = String.format(

				"alter table %s "

						+ "add column if not exists %s_%s numeric" + ";",

						RAW_LOG_SCORES,

						LOG_SCORE, partSuffix);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", addLogScoreColumnCmd);
		return addLogScoreColumnCmd;
	}

	private static String getUpdateLogScoreCmd(final String partSuffix) {
		final String updateLogScoresCmd = String.format(

				"update %s "

						+ "set %s_%s = ? "

						+ "where %s = ?" + ";",

						RAW_LOG_SCORES,

						LOG_SCORE, partSuffix,

						STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", updateLogScoresCmd);
		return updateLogScoresCmd;
	}

}
