package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.BHT_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.DEINDEXED;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.DEINDEXED_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.DISCONTINUED;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.DISCONTINUED_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.FOREIGN_STREAM_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_CATEGORY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RATING_RANK;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_RATINGS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_STREAMS;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.dblp.streamdb.Conference;
import org.dblp.streamdb.Ranking;

import com.google.common.collect.ImmutableList;

public class StreamDbWrapper extends CoreComponent {

	private static Logger LOGGER = LogManager.getLogger(StreamDbWrapper.class);
	private static final int BATCH_SIZE = 1000;

	public static void wrapTo(final Connection connection) {

		final ImmutableList<Conference> conferences = ImmutableList
				.copyOf(ExternalDataHub.getStreamDb().getConferences());

		wrapConferenceStreamsTo(connection, conferences);
		wrapConferenceRatingsTo(connection, conferences);

		ExternalDataHub.flushStreamDb();
	}

	private static void wrapConferenceStreamsTo(final Connection connection,
			final ImmutableList<Conference> conferences) {

		reCreateTable(connection, CONFERENCE_STREAMS, getCreateConferenceStreamsCmd(), true);

		final String insertStreamLogFormat = "{}: Batch-inserting conference streams into table '{}'";
		final Instant insertStreamStart = Instant.now();
		LOGGER.info(insertStreamLogFormat, "START", CONFERENCE_STREAMS);

		int streamCount = 0;

		try (final PreparedStatement insertStream = connection.prepareStatement(getInsertStreamCmd())) {

			for (final Conference conference : conferences) {

				insertStream.setString(1, conference.getKey());
				insertStream.setString(2, conference.getIndexPageKey());
				insertStream.setBoolean(3, conference.isDeindexed());
				insertStream.setDate(4, getSqlDate(conference.getDeindexedDate()));
				insertStream.setBoolean(5, conference.isDiscontinued());
				insertStream.setDate(6, getSqlDate(conference.getDiscDate()));

				insertStream.addBatch();

				if ((++streamCount % BATCH_SIZE) == 0) {
					LOGGER.info("Processed {} streams...", streamCount);
					insertStream.executeBatch();
				}
			}
			insertStream.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertStreamLogFormat, "FAILED", CONFERENCE_STREAMS), e);
			System.exit(1);
		}
		LOGGER.info(insertStreamLogFormat + " (Duration: {})", "END", CONFERENCE_STREAMS,
				Duration.between(insertStreamStart, Instant.now()));
	}

	private static void wrapConferenceRatingsTo(final Connection connection,
			final ImmutableList<Conference> conferences) {

		reCreateTable(connection, CONFERENCE_RATINGS, getCreateConferenceRatingsCmd());

		final String insertRatingsLogFormat = "{}: Batch-inserting conference ratings into table '{}'";
		final Instant insertRatingsStart = Instant.now();
		LOGGER.info(insertRatingsLogFormat, "START", CONFERENCE_STREAMS);

		int ratingCount = 0;

		try (final PreparedStatement insertRating = connection.prepareStatement(getInsertRatingCmd())) {

			for (final Conference conference : conferences) {

				for (final Ranking ranking : conference.getRankings()) {

					insertRating.setString(1, ranking.key);
					insertRating.setString(2, conference.getKey());
					insertRating.setString(3, ranking.id);
					insertRating.setString(4, ranking.cat);
					insertRating.setString(5, ranking.rank);

					insertRating.addBatch();

					if ((++ratingCount % BATCH_SIZE) == 0) {
						LOGGER.info("Processed {} ratings...", ratingCount);
						insertRating.executeBatch();
					}
				}
			}
			insertRating.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertRatingsLogFormat, "FAILED", CONFERENCE_STREAMS), e);
			System.exit(1);
		}
		LOGGER.info(insertRatingsLogFormat + " (Duration: {})", "END", CONFERENCE_STREAMS,
				Duration.between(insertRatingsStart, Instant.now()));
	}

	private static Date getSqlDate(final String date) {
		return (date == null) ? null : Date.valueOf(LocalDate.parse(date));
	}

	private static String getCreateConferenceStreamsCmd() {
		return String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, " + "%s varchar(255) not null, " + "%s varchar(255) not null, "

						+ "%s boolean not null, " + "%s date, "

						+ "%s boolean not null, " + "%s date " + ")" + ";",

				CONFERENCE_STREAMS,

				GENERIC_ID, STREAM_KEY, BHT_KEY,

				DEINDEXED, DEINDEXED_DATE,

				DISCONTINUED, DISCONTINUED_DATE);
	}

	private static String getInsertStreamCmd() {
		return String.format(

				"insert into %s "

						+ "( %s, %s, %s, %s, %s, %s ) "

						+ "values (?, ?, ?, ?, ?, ?)" + ";",
				CONFERENCE_STREAMS,

				STREAM_KEY, BHT_KEY, DEINDEXED, DEINDEXED_DATE, DISCONTINUED, DISCONTINUED_DATE);
	}

	private static String getCreateConferenceRatingsCmd() {

		return String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, " + "%s varchar(255) not null, " + "%s varchar(255) not null, "

						+ "%s varchar(255), " + "%s varchar(255), " + "%s varchar(255) not null "

						+ ")" + ";",

				CONFERENCE_RATINGS,

				GENERIC_ID, RATING_KEY, STREAM_KEY,

				FOREIGN_STREAM_ID, RATING_CATEGORY, RATING_RANK);
	}

	private static String getInsertRatingCmd() {
		return String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s, %s, %s" + " ) "

						+ "values (?, ?, ?, ?, ?)" + ";",

				CONFERENCE_RATINGS,

				RATING_KEY, STREAM_KEY, FOREIGN_STREAM_ID, RATING_CATEGORY, RATING_RANK);
	}

}
