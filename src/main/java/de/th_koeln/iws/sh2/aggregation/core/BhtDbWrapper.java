package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.BHT_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_CITES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_STREAMS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableSet;

/**
 * Implementation of the {@link CoreComponent} to wrap BhtDb data.
 * 
 * <p>
 * Depends on {@code CONFERENCE_STREAMS}.
 * 
 * <p>
 * Creates {@code CONFERENCE_CITES}, {@code EVENT_VIEW}.
 * 
 * <p>
 * This dblp-specific interface is used to map each conference
 * {@code STREAM_KEY} to all its related {@code CITE_KEY} values via the
 * {@code BHT_KEY} given by the StreamDb. Intuitively, a {@code CITE_KEY} is the
 * {@code RECORD_KEY} of the proceedings record of the event of a conference.
 * Here, intuition entails that this is not always the case. There are events
 * which are published in a different mode, e.g. 'select * from
 * {@code CONFERENCE_CITES} where {@code CITE_KEY} not like 'conf%' yields
 * values of the pattern 'journals%' or 'books%'. The column {@code BHT_KEY}
 * allows for joins with {@code CONFERENCE_STREAMS}.
 *
 * @author michels
 *
 */
public class BhtDbWrapper extends CoreComponent {

	private static final Logger LOGGER = LogManager.getLogger(BhtDbWrapper.class);
	private static final int BATCH_SIZE = 10000;

	public static void wrapTo(final Connection connection) {

		wrapConferenceCitesTo(connection);

		ExternalDataHub.flushBhtDb();
	}

	private static void wrapConferenceCitesTo(final Connection connection) {

		reCreateTable(connection, CONFERENCE_CITES, getCreateConferenceCitesCmd(), true);

		final String insertCitesLogFormat = "{}: Batch-inserting conference cites into table '{}'";
		final Instant insertCitesStart = Instant.now();
		LOGGER.info(insertCitesLogFormat, "START", CONFERENCE_CITES);

		try (final PreparedStatement selectConferenceStreams = connection
				.prepareStatement(getSelectConferenceStreamsCmd());
				final PreparedStatement insertCite = connection.prepareStatement(getInsertCiteCmd())) {

			final ResultSet conferenceStreams = selectConferenceStreams.executeQuery();

			int citeCount = 0;

			while (conferenceStreams.next()) {

				final String bhtKey = conferenceStreams.getString(BHT_KEY);
				Set<String> citeKeys = ImmutableSet.copyOf(ExternalDataHub.getBhtDb().getCiteKeys(bhtKey));

				for (final String citeKey : citeKeys) {

					insertCite.setString(1, citeKey);
					insertCite.setString(2, bhtKey);

					insertCite.addBatch();

					if ((++citeCount % BATCH_SIZE) == 0) {
						LOGGER.info("Processed {} cites...", citeCount);
						insertCite.executeBatch();
					}
				}
			}
			insertCite.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertCitesLogFormat, "FAILED", CONFERENCE_CITES), e);
			System.exit(1);
		}
		LOGGER.info(insertCitesLogFormat + " (Duration: {})", "END", CONFERENCE_CITES,
				Duration.between(insertCitesStart, Instant.now()));
	}

	private static String getCreateConferenceCitesCmd() {
		return String.format(

				"create table if not exists %s " + "( "

						+ "%s bigserial primary key, " + "%s varchar(255) not null, " + "%s varchar(255) not null"
						+ " )" + ";",

				CONFERENCE_CITES,

				GENERIC_ID, CITE_KEY, BHT_KEY);
	}

	private static String getSelectConferenceStreamsCmd() {
		return String.format(

				"select * from %s" + ";",

				CONFERENCE_STREAMS);
	}

	private static String getInsertCiteCmd() {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

				CONFERENCE_CITES,

				CITE_KEY, BHT_KEY);
	}

}
