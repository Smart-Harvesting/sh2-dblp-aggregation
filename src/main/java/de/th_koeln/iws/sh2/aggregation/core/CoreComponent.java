package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_STREAMS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

public abstract class CoreComponent {

	protected static Logger LOGGER = LogManager.getLogger(CoreComponent.class);

	protected static void reCreateTable(final Connection connection, final String tableName,
			final String tableCreationCmd) {
		reCreateTable(connection, tableName, tableCreationCmd, false);
	}

	protected static void reCreateTable(final Connection connection, final String tableName,
			final String tableCreationCmd, final boolean cascade) {

		try (final PreparedStatement dropTable = connection.prepareStatement(getDropTableCmd(tableName, cascade));
				final PreparedStatement createTable = connection.prepareStatement(tableCreationCmd)) {
			LOGGER.info("Dropping table '{}'", tableName);
			dropTable.executeUpdate();
			LOGGER.info("Re-creating table '{}'", tableName);
			createTable.executeUpdate();
		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage("Unable to re-create table '{}'", tableName), e);
			System.exit(1);
		}
	}

	protected static void createTable(final Connection connection, final String tableName,
			final String tableCreationCmd) {
		try (final PreparedStatement createTable = connection.prepareStatement(tableCreationCmd)) {
			LOGGER.info("Re-creating table '{}'", tableName);
			createTable.executeUpdate();
		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage("Unable to create table '{}'", tableName), e);
			System.exit(1);
		}
	}

	protected static void materializeView(final Connection connection, final String viewName,
			final String materializeViewCmd) {
		materializeView(connection, viewName, materializeViewCmd, false);
	}

	protected static void materializeView(Connection connection, String viewName, String materializeViewCmd,
			boolean cascade) {
		final String materializeLogFormat = "{}: Materializing view '{}'";
		final Instant materializeStart = Instant.now();
		LOGGER.info(materializeLogFormat, "START", viewName);

		try (final PreparedStatement dropView = connection
				.prepareStatement(getDropMaterializedViewCmd(viewName, cascade));
				final PreparedStatement materializeView = connection.prepareStatement(materializeViewCmd)) {
			LOGGER.info("Dropping view '{}'", viewName);
			dropView.execute();
			LOGGER.info("Re-creating view '{}'", viewName);
			materializeView.execute();
		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(materializeLogFormat, "FAILED", viewName), e);
			System.exit(1);
		}
		LOGGER.info(materializeLogFormat + " (Duration: {})", "END", viewName,
				Duration.between(materializeStart, Instant.now()));
	}

	protected static void createPartitionByDate(final Connection connection, final String tableName,
			final LocalDate partitionDate) {

		try (final PreparedStatement createPartitionByDate = connection
				.prepareStatement(getCreatePartitionByDateCmd(tableName, partitionDate))) {
			createPartitionByDate.executeUpdate();

		} catch (SQLException e) {
			LOGGER.error("Failed to partition table appropriately", e);
			LOGGER.error(e.getNextException());
		}
	}

	protected static void createIndex(final Connection connection, final String tableName,
			final ImmutableList<String> columnNames) {

		final String indexLogFormat = "{}: Creating index for column(s) '{}' of table '{}'";
		final Instant indexStart = Instant.now();
		LOGGER.info(indexLogFormat, "START", columnNames, tableName);

		try (final PreparedStatement createIndex = connection
				.prepareStatement(getCreateIndexCmd(tableName, columnNames))) {
			createIndex.executeUpdate();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(indexLogFormat, "FAILED", columnNames, tableName), e);
			System.exit(1);
		}
		LOGGER.info(indexLogFormat + " (Duration: {})", "END", columnNames, tableName,
				Duration.between(indexStart, Instant.now()));
	}

	protected static ImmutableSortedSet<String> getStreamKeys(final Connection connection) {

		final Set<String> streamKeys = Sets.newTreeSet();

		final String aggregateLogFormat = "{}: Aggregating stream keys from table '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", CONFERENCE_STREAMS);

		try (final PreparedStatement aggregateStreamKeys = connection.prepareStatement(getAggregateStreamKeysCmd())) {

			final ResultSet streamKeysResults = aggregateStreamKeys.executeQuery();

			while (streamKeysResults.next()) {
				streamKeys.add(streamKeysResults.getString(STREAM_KEY));
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", CONFERENCE_STREAMS), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", CONFERENCE_STREAMS,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableSortedSet.copyOf(streamKeys);
	}

	private static String getDropTableCmd(final String tableName, final boolean cascade) {
		return String.format(

				"DROP TABLE IF EXISTS %s " + "%s" + ";",

				tableName, (cascade) ? "cascade" : "");

	}

	private static String getDropMaterializedViewCmd(final String viewName, final boolean cascade) {
		return String.format(

				"DROP MATERIALIZED VIEW IF EXISTS %s " + "%s" + ";",

				viewName, (cascade) ? "cascade" : "");
	}

	private static String getCreatePartitionByDateCmd(final String tableName, final LocalDate partitionDate) {
		return String.format(

				"create table if not exists %s_y%dm%02d "

						+ "partition of %s for values "

						+ "from ('%d-%02d-01') "

						+ "to ('%d-%02d-01')" + ";",

				tableName, partitionDate.getYear(), partitionDate.getMonth().getValue(),

				tableName,

				partitionDate.getYear(), partitionDate.getMonth().getValue(),

				partitionDate.plusMonths(1).getYear(), partitionDate.plusMonths(1).getMonth().getValue());
	}

	private static String getCreateIndexCmd(final String tableName, ImmutableList<String> columnNames) {
		return String.format("create index if not exists %s_%s_idx on %s using btree (%s);", tableName,
				String.join("_", columnNames), tableName, String.join(", ", columnNames));
	}

	private static String getAggregateStreamKeysCmd() {

		final String aggregateStreamKeysCmd = String.format(

				"select distinct %s from %s;",

				STREAM_KEY, CONFERENCE_STREAMS);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", aggregateStreamKeysCmd);
		return aggregateStreamKeysCmd;
	}

}
