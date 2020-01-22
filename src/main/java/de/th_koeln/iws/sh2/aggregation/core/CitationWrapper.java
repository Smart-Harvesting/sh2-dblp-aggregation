package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITING_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITING_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.TARGET_EVENT_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.TARGET_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.TARGET_PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.INCOMING_CITATIONS;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.TOC_VIEW;

import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.dblp.citations.auxiliary.AuxData;
import org.dblp.citations.auxiliary.ReadonlyAuxCollection;
import org.dblp.citations.auxiliary.item.AuxItem;
import org.dblp.citations.auxiliary.item.CitedByAuxItem;

import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;

public class CitationWrapper extends CoreComponent {

	private static final Logger LOGGER = LogManager.getLogger(CitationWrapper.class);
	private static final int BATCH_SIZE = 100000;

	public static void wrapTo(final Connection connection, final int lteYear) {

		ReadonlyAuxCollection oagSingleReadOnlyHandle = getOagSingleReadOnlyHandle();
		reCreateTable(connection, INCOMING_CITATIONS, getCreateIncomingCitationsCmd());

		final String insertLogFormat = "{}: Batch-inserting into table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", INCOMING_CITATIONS);

		SetMultimap<String, String> streamToRecords = TreeMultimap.create();
		int citedByCount = 0;

		try (final PreparedStatement selectRecordsOfStreamInYear = connection
				.prepareStatement(getSelectTocRecordsCmd(lteYear));
				final PreparedStatement insertIncCitation = connection
						.prepareStatement(getInsertIncomingCitationCmd())) {

			final ResultSet recordsOfStreamInYear = selectRecordsOfStreamInYear.executeQuery();

			while (recordsOfStreamInYear.next()) {

				final String streamKey = recordsOfStreamInYear.getString(STREAM_KEY);
				final String citeKey = recordsOfStreamInYear.getString(CITE_KEY);
				final String targetKey = recordsOfStreamInYear.getString(RECORD_KEY);
				final int targetPublicationYear = recordsOfStreamInYear.getInt(PUBLICATION_YEAR);
				final int targetEventYear = recordsOfStreamInYear.getInt(EVENT_YEAR);

				if (streamToRecords.put(streamKey, targetKey)) {

					final AuxData auxData = oagSingleReadOnlyHandle.getAuxDataForKey(targetKey);

					if (auxData != null) {

						for (final AuxItem item : auxData.getItems()) {

							if (item instanceof CitedByAuxItem) {

								final CitedByAuxItem citing = (CitedByAuxItem) item;

								insertIncCitation.setString(1, streamKey);
								insertIncCitation.setString(2, citeKey);
								insertIncCitation.setString(3, targetKey);
								insertIncCitation.setInt(4, targetPublicationYear);
								insertIncCitation.setInt(5, targetEventYear);
								insertIncCitation.setString(6, citing.getKey());
								insertIncCitation.setInt(7, getCitingYear(connection, lteYear, citing.getKey()));

								insertIncCitation.addBatch();

								if ((++citedByCount % BATCH_SIZE) == 0) {
									LOGGER.info("Processed {} incoming citations...", citedByCount);
									insertIncCitation.executeBatch();
								}
							}
						}
					}
				}
			}
			insertIncCitation.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", INCOMING_CITATIONS), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", INCOMING_CITATIONS,
				Duration.between(insertStart, Instant.now()));
	}

	private static int getCitingYear(final Connection connection, final int lteYear, final String citingKey)
			throws SQLException {

		int citingYear = 3333;

		if (citingKey.contains("/")) {

			try (final PreparedStatement selectCitingYear = connection
					.prepareStatement(getSelectCitingYearCmd(lteYear))) {

				selectCitingYear.setString(1, citingKey);

				final ResultSet citingYears = selectCitingYear.executeQuery();

				if (citingYears.next()) {
					citingYear = citingYears.getInt(PUBLICATION_YEAR);
				}
			}
		}
		return citingYear;
	}

	private static ReadonlyAuxCollection getOagSingleReadOnlyHandle() {
		ReadonlyAuxCollection oagSingleReadOnlyHandle = null;
		try {
			oagSingleReadOnlyHandle = ExternalDataHub.getOagSingleReadOnlyHandle();
		} catch (NoSuchFileException e) {
			LOGGER.fatal("Unable to access OAG auxiliary data for dblp", e);
			System.exit(1);
		}
		return oagSingleReadOnlyHandle;
	}

	private static String getCreateIncomingCitationsCmd() {
		final String createIncomingCitationsCmd = String.format(

				"create table if not exists %s " + "( "

						+ "%s bigserial, " + "%s varchar(255), " + "%s varchar(255), "

						+ "%s varchar(255), " + "%s int, " + "%s int, "

						+ "%s varchar(255), " + "%s int " + ") " + ";",

				INCOMING_CITATIONS,

				GENERIC_ID, STREAM_KEY, CITE_KEY,

				TARGET_KEY, TARGET_PUBLICATION_YEAR, TARGET_EVENT_YEAR,

				CITING_KEY, CITING_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createIncomingCitationsCmd);
		return createIncomingCitationsCmd;
	}

	private static String getSelectTocRecordsCmd(final int lteYear) {
		final String selectTocRecordsCmd = String.format(

				"select * from %s "

						+ "where %s <= '%d-12-31'" + ";",

				TOC_VIEW,

				C_DATE, lteYear);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectTocRecordsCmd);
		return selectTocRecordsCmd;
	}

	private static String getInsertIncomingCitationCmd() {
		final String insertIncomingCitationCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s, %s, %s, %s, %s" + " ) "

						+ "values (?, ?, ?, ?, ?, ?, ?) " + ";",

				INCOMING_CITATIONS,

				STREAM_KEY, CITE_KEY,

				TARGET_KEY, TARGET_PUBLICATION_YEAR, TARGET_EVENT_YEAR,

				CITING_KEY, CITING_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertIncomingCitationCmd);
		return insertIncomingCitationCmd;
	}

	private static String getSelectCitingYearCmd(final int lteYear) {
		final String selectCitingYearCmd = String.format(

				"select %s "

						+ "from %s hdblp "

						+ "where hdblp.%s <= '%d-12-31'"

						+ "and hdblp.%s = ?" + ";",

				PUBLICATION_YEAR,

				HISTORICAL_RECORDS,

				C_DATE, lteYear,

				RECORD_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectCitingYearCmd);
		return selectCitingYearCmd;
	}

}
