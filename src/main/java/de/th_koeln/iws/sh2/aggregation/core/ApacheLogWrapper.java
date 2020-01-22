package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_CIRCLES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_END;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_START;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_SWAPS;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_TIME_PER_PAGE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_TOTAL_PAGES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSIONS_TOTAL_TIME;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSION_STRUCTURE_LOG_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSION_STRUCTURE_SESSION_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.SESSION_STRUCTURE_USER_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.USERS_CLIENT_HOST;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.USERS_FIRST_OCCURRENCE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.USERS_LAST_OCCURRENCE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.USERS_USER_AGENT;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_LOG_USERS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SESSIONS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SESSION_STRUCTURE;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.USERS;
import static org.sh2.ala.core.util.ColumnNames.BYTES;
import static org.sh2.ala.core.util.ColumnNames.CLIENT_HOST;
import static org.sh2.ala.core.util.ColumnNames.CLIENT_USER;
import static org.sh2.ala.core.util.ColumnNames.GENERIC_TIMESTAMP;
import static org.sh2.ala.core.util.ColumnNames.GET_REQUEST;
import static org.sh2.ala.core.util.ColumnNames.LOGNAME;
import static org.sh2.ala.core.util.ColumnNames.REFERER;
import static org.sh2.ala.core.util.ColumnNames.SERVER_NAME;
import static org.sh2.ala.core.util.ColumnNames.STATUS;
import static org.sh2.ala.core.util.ColumnNames.USER_AGENT;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import de.th_koeln.iws.sh2.aggregation.model.DblpLog;
import de.th_koeln.iws.sh2.aggregation.model.DblpLogSession;

public class ApacheLogWrapper extends CoreComponent {

	private static Logger LOGGER = LogManager.getLogger(ApacheLogWrapper.class);
	private static final int BATCH_SIZE = 100000;

	public static void wrapTo(final Connection connection, final String logsTableName, final String logsPartitionName)
			throws SQLException {

		//		wrapLogUsersTo(connection, logsTableName);

		reCreateTable(connection, SESSION_STRUCTURE, getCreateSessionStructureCmd());
		reCreateTable(connection, SESSIONS, getCreateSessionsCmd());

		final String wrapSessionsFormat = "{}: Analysing log-session structure for table '{}'";
		final Instant wrapSessionsStart = Instant.now();
		LOGGER.info(wrapSessionsFormat, "START", logsPartitionName);

		final Set<Long> users = Sets.newHashSet();
		long sessionId = 0;
		int logCount = 0;

		try (final PreparedStatement joinLogsUsers = connection.prepareStatement(getJoinLogsUsersCmd(logsPartitionName));
				final PreparedStatement insertSession = connection.prepareStatement(getInsertSessionCmd());
				final PreparedStatement insertSessionLog = connection.prepareStatement(getInsertSessionLogCmd())) {

			final String joinLogsUsersFormat = "{}: Joining logs and users for table '{}'";
			final Instant joinLogsUsersStart = Instant.now();
			LOGGER.info(joinLogsUsersFormat, "START", logsPartitionName);

			connection.setAutoCommit(false);
			joinLogsUsers.setFetchSize(100000);

			final ResultSet joinLogsAndUsersResults = joinLogsUsers.executeQuery();

			LOGGER.info(joinLogsUsersFormat + " (Duration: {})", "END", logsPartitionName,
					Duration.between(joinLogsUsersStart, Instant.now()));

			final String persistSessionsFormat = "{}: Persisting session data for table '{}'";
			final Instant persistSessionsStart = Instant.now();
			LOGGER.info(persistSessionsFormat, "START", logsPartitionName);

			DblpLogSession currentUserLogSession = null;

			while (joinLogsAndUsersResults.next()) {

				final long userId = joinLogsAndUsersResults.getLong(SESSION_STRUCTURE_USER_ID);

				if (users.add(userId)) {
					if (currentUserLogSession != null) {
						insertSession(connection, currentUserLogSession, insertSession);
						logCount = insertSessionLogs(connection, currentUserLogSession, insertSessionLog, logCount);
					}
					currentUserLogSession = new DblpLogSession(++sessionId, userId);
				}

				final DblpLog newLog = getDblpLogFromResults(joinLogsAndUsersResults);

				if (!currentUserLogSession.addDblpLog(newLog)) {
					insertSession(connection, currentUserLogSession, insertSession);
					logCount = insertSessionLogs(connection, currentUserLogSession, insertSessionLog, logCount);
					currentUserLogSession = new DblpLogSession(++sessionId, userId);
					currentUserLogSession.addDblpLog(newLog);
				}
			}

			insertSession.executeBatch();
			insertSessionLog.executeBatch();

			LOGGER.info(persistSessionsFormat + " (Duration: {})", "END", logsPartitionName,
					Duration.between(persistSessionsStart, Instant.now()));

			connection.commit();
			connection.setAutoCommit(true);

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(wrapSessionsFormat, "FAILED", logsPartitionName), e);
			System.exit(1);
		}
		LOGGER.info(wrapSessionsFormat + " (Duration: {})", "END", logsPartitionName,
				Duration.between(wrapSessionsStart, Instant.now()));
	}

	private static void wrapLogUsersTo(final Connection connection, final String logsTableName) {

		wrapRawLogUsersTo(connection, logsTableName);

		reCreateTable(connection, USERS, getCreateLogUsersCmd());

		final String insertLogFormat = "{}: Batch-inserting log users into table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.debug(insertLogFormat, "START", USERS);

		try (final PreparedStatement insertLogUsers = connection.prepareStatement(getInsertLogUsersCmd())) {
			insertLogUsers.executeUpdate();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", USERS), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", USERS, Duration.between(insertStart, Instant.now()));

		createIndex(connection, USERS, ImmutableList.of(GENERIC_ID));
		createIndex(connection, USERS, ImmutableList.of(USERS_FIRST_OCCURRENCE));
		createIndex(connection, USERS, ImmutableList.of(USERS_LAST_OCCURRENCE));
		createIndex(connection, USERS, ImmutableList.of(USERS_CLIENT_HOST));
		createIndex(connection, USERS, ImmutableList.of(USERS_USER_AGENT));
		createIndex(connection, USERS, ImmutableList.of(USERS_CLIENT_HOST, USERS_USER_AGENT));
	}

	private static void wrapRawLogUsersTo(final Connection connection, final String logsTableName) {

		reCreateTable(connection, RAW_LOG_USERS, getCreateRawLogUsersCmd());

		final String insertRawLogUsersLogFormat = "{}: Batch-inserting into table '{}'";
		final Instant insertRawLogUsersStart = Instant.now();
		LOGGER.info(insertRawLogUsersLogFormat, "START", RAW_LOG_USERS);

		try (final PreparedStatement insertLogUsers = connection.prepareStatement(getInsertRawLogUsersCmd(logsTableName))) {
			insertLogUsers.executeUpdate();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertRawLogUsersLogFormat, "FAILED", RAW_LOG_USERS), e);
			System.exit(1);
		}
		LOGGER.info(insertRawLogUsersLogFormat + " (Duration: {})", "END", RAW_LOG_USERS,
				Duration.between(insertRawLogUsersStart, Instant.now()));

		createIndex(connection, RAW_LOG_USERS, ImmutableList.of(GENERIC_ID));
		createIndex(connection, RAW_LOG_USERS, ImmutableList.of(GENERIC_DATE));
		createIndex(connection, RAW_LOG_USERS, ImmutableList.of(USERS_CLIENT_HOST));
		createIndex(connection, RAW_LOG_USERS, ImmutableList.of(USERS_USER_AGENT));
		createIndex(connection, RAW_LOG_USERS, ImmutableList.of(USERS_CLIENT_HOST, USERS_USER_AGENT));
		createIndex(connection, RAW_LOG_USERS, ImmutableList.of(USERS_CLIENT_HOST, USERS_USER_AGENT, GENERIC_DATE));
	}

	private static DblpLog getDblpLogFromResults(final ResultSet joinLogsAndUsersResults) throws SQLException {
		return new DblpLog(joinLogsAndUsersResults.getLong(GENERIC_ID), joinLogsAndUsersResults.getString(SERVER_NAME),
				joinLogsAndUsersResults.getString(CLIENT_HOST), joinLogsAndUsersResults.getString(LOGNAME),
				joinLogsAndUsersResults.getString(CLIENT_USER),
				joinLogsAndUsersResults.getDate(GENERIC_DATE).toLocalDate(),
				joinLogsAndUsersResults.getTimestamp(GENERIC_TIMESTAMP).toLocalDateTime(),
				joinLogsAndUsersResults.getString(GET_REQUEST), joinLogsAndUsersResults.getInt(STATUS),
				joinLogsAndUsersResults.getInt(BYTES), joinLogsAndUsersResults.getString(REFERER),
				joinLogsAndUsersResults.getString(USER_AGENT));
	}

	private static void insertSession(final Connection connection, final DblpLogSession logSession,
			final PreparedStatement insertSession) throws SQLException {

		final LocalDate sessionStartDate = logSession.getStart().toLocalDate();
		createPartitionByDate(connection, SESSIONS, sessionStartDate);

		insertSession.setLong(1, logSession.getSessionId());
		insertSession.setDate(2, Date.valueOf(sessionStartDate));
		insertSession.setTimestamp(3, Timestamp.valueOf(logSession.getStart()));
		insertSession.setTimestamp(4, Timestamp.valueOf(logSession.getEnd()));
		insertSession.setLong(5, logSession.getTotalTime());
		insertSession.setInt(6, logSession.getTotalPages());
		insertSession.setDouble(7, logSession.getTimePerPage());
		insertSession.setInt(8, logSession.getCircles());
		insertSession.setInt(9, logSession.getSwaps());

		insertSession.addBatch();

		if ((logSession.getSessionId() % BATCH_SIZE) == 0) {
			LOGGER.info("Processed {} sessions...", logSession.getSessionId());
			insertSession.executeBatch();
		}
	}

	private static int insertSessionLogs(final Connection connection, final DblpLogSession currentUserLogSession,
			final PreparedStatement insertSessionLog, int logCount) throws SQLException {

		for (DblpLog log : currentUserLogSession.getLogs()) {

			final LocalDate logDate = log.getDate();
			createPartitionByDate(connection, SESSION_STRUCTURE, logDate);

			insertSessionLog.setDate(1, Date.valueOf(logDate));
			insertSessionLog.setLong(2, currentUserLogSession.getSessionId());
			insertSessionLog.setLong(3, log.getLogId());

			insertSessionLog.addBatch();

			if ((++logCount % BATCH_SIZE) == 0) {
				LOGGER.info("Processed {} logs...", logCount);
				insertSessionLog.executeBatch();
			}
		}
		return logCount;
	}

	private static String getCreateRawLogUsersCmd() {
		return String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, %s date, "

						+ "%s  varchar(255) not null, " + "%s varchar(5000) not null "

						+ ")" + ";",

						RAW_LOG_USERS,

						GENERIC_ID, GENERIC_DATE,

						USERS_CLIENT_HOST, USERS_USER_AGENT);
	}

	private static String getInsertRawLogUsersCmd(final String logsTableName) {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s " + ") "

						+ "select distinct %s, %s, %s "

						+ "from %s" + ";",

						RAW_LOG_USERS,

						GENERIC_DATE, USERS_CLIENT_HOST, USERS_USER_AGENT,

						GENERIC_DATE, USERS_CLIENT_HOST, USERS_USER_AGENT,

						logsTableName);
	}

	private static String getCreateLogUsersCmd() {
		return String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, %s date, %s date, "

						+ "%s  varchar(255) not null, " + "%s varchar(5000) not null "

						+ ")" + ";",

						USERS,

						GENERIC_ID, USERS_FIRST_OCCURRENCE, USERS_LAST_OCCURRENCE,

						USERS_CLIENT_HOST, USERS_USER_AGENT);
	}

	private static String getInsertLogUsersCmd() {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s, %s " + ") "

						+ "select %s, %s, %s, %s from " + "( "

						+ "select distinct on ( " + "%s, %s" + " ) "

						+ "%s, %s, %s %s "

						+ "from %s "

						+ "order by %s, %s, %s asc" + " ) t1 "

						+ "join ( "

						+ "select distinct on ( " + "%s, %s" + " ) "

						+ "%s, %s, %s %s "

						+ "from %s "

						+ "order by %s, %s, %s desc" + " ) t2 "

						+ "using ( " + "%s, %s" + " )" + ";",

						USERS,

						USERS_FIRST_OCCURRENCE, USERS_LAST_OCCURRENCE, USERS_CLIENT_HOST, USERS_USER_AGENT,

						USERS_FIRST_OCCURRENCE, USERS_LAST_OCCURRENCE, USERS_CLIENT_HOST, USERS_USER_AGENT,

						USERS_CLIENT_HOST, USERS_USER_AGENT,

						USERS_CLIENT_HOST, USERS_USER_AGENT, GENERIC_DATE, USERS_FIRST_OCCURRENCE,

						RAW_LOG_USERS,

						USERS_CLIENT_HOST, USERS_USER_AGENT, GENERIC_DATE,

						USERS_CLIENT_HOST, USERS_USER_AGENT,

						USERS_CLIENT_HOST, USERS_USER_AGENT, GENERIC_DATE, USERS_LAST_OCCURRENCE,

						RAW_LOG_USERS,

						USERS_CLIENT_HOST, USERS_USER_AGENT, GENERIC_DATE,

						USERS_CLIENT_HOST, USERS_USER_AGENT);
	}

	private static String getCreateSessionsCmd() {
		return String.format(

				"create table if not exists %s ( "

						+ "%s bigint not null, " + "%s date, "

						+ "%s timestamp with time zone not null, " + "%s timestamp with time zone not null, "

						+ "%s bigint not null, " + "%s int not null, " + "%s double precision not null, "

						+ "%s int not null, " + "%s int not null "

						+ ") " + "partition by range (%s)" + ";",

						SESSIONS,

						GENERIC_ID, GENERIC_DATE,

						SESSIONS_START, SESSIONS_END,

						SESSIONS_TOTAL_TIME, SESSIONS_TOTAL_PAGES, SESSIONS_TIME_PER_PAGE,

						SESSIONS_CIRCLES, SESSIONS_SWAPS,

						GENERIC_DATE);
	}

	private static String getCreateSessionStructureCmd() {
		return String.format(

				"create table if not exists %s ( "

						+ "%s bigserial, " + "%s date, "

						+ "%s bigint, " + "%s bigint "

						+ ") " + "partition by range (%s)" + ";",

						SESSION_STRUCTURE,

						GENERIC_ID, GENERIC_DATE,

						SESSION_STRUCTURE_SESSION_ID, SESSION_STRUCTURE_LOG_ID,

						GENERIC_DATE);
	}

	private static String getJoinLogsUsersCmd(final String logsPartitionName) {
		return String.format(

				"select * from ( "

						+ "select %s.%s as %s, " + "%s.* "

						+ "from %s join %s "

						+ "using (%s, %s)" + ") t1 "

						+ "order by %s, %s" + ";",

						USERS, GENERIC_ID, SESSION_STRUCTURE_USER_ID, logsPartitionName,

						logsPartitionName, USERS,

						USERS_CLIENT_HOST, USERS_USER_AGENT,

						SESSION_STRUCTURE_USER_ID, GENERIC_ID);
	}

	private static String getInsertSessionCmd() {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s, %s, %s, %s, %s, %s, %s" + " ) "

						+ "VALUES ( " + "?, ?, ?, ?, ?, ?, ?, ?, ?" + " ) " + ";",

						SESSIONS,

						GENERIC_ID, GENERIC_DATE, SESSIONS_START, SESSIONS_END,

						SESSIONS_TOTAL_TIME, SESSIONS_TOTAL_PAGES, SESSIONS_TIME_PER_PAGE,

						SESSIONS_CIRCLES, SESSIONS_SWAPS);
	}

	private static String getInsertSessionLogCmd() {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s" + " ) "

						+ "values (?, ?, ?)" + ";",

						SESSION_STRUCTURE,

						GENERIC_DATE, SESSION_STRUCTURE_SESSION_ID, SESSION_STRUCTURE_LOG_ID);
	}

}