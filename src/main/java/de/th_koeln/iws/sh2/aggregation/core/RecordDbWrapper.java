package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.AUTHOR_COUNT;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.BHT_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.DEINDEXED;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.DISCONTINUED;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PERSON_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_COUNT;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_CITES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_RECORDS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.CONFERENCE_STREAMS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SIGNATURES;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.DETAIL_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PERSON_YEAR_RECORD_COUNT_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_AUTHOR_COUNT_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_RECORD_COUNT_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.TOC_VIEW;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.dblp.mmdb.Person;
import org.dblp.mmdb.Publication;
import org.dblp.mmdb.TableOfContents;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;

public class RecordDbWrapper extends CoreComponent {

	private static final Logger LOGGER = LogManager.getLogger(RecordDbWrapper.class);
	private static final int BATCH_SIZE = 100000;

	public static void wrapTo(final Connection connection, final int lteYear) {

		final ImmutableList<Person> persons = ImmutableList.copyOf(ExternalDataHub.getRecordDb().getPersons());

		wrapSignaturesTo(connection, persons);
		wrapConferenceRecordsTo(connection);

		materializeView(connection, PROCEEDINGS_VIEW, getMaterializeProceedingsViewCmd(), true);
		materializeView(connection, TOC_VIEW, getMaterializeTocViewCmd(lteYear), true);
		materializeView(connection, DETAIL_VIEW, getMaterializeDetailViewCmd(), true);

		materializeView(connection, PROCEEDINGS_RECORD_COUNT_VIEW, getMaterializeProcRecordCountViewCmd(lteYear), true);
		materializeView(connection, PROCEEDINGS_AUTHOR_COUNT_VIEW, getMaterializeProcAuthorCountViewCmd(lteYear), true);

		materializeView(connection, PERSON_YEAR_RECORD_COUNT_VIEW, getMaterializePersonRecordCountViewCmd(lteYear),
				true);

		ExternalDataHub.flushRecordDb();

	}

	private static void wrapSignaturesTo(final Connection connection, final ImmutableList<Person> persons) {

		reCreateTable(connection, SIGNATURES, getCreateSignaturesCmd(), true);

		final String insertSignaturesFormat = "{}: Batch-inserting signatures into table '{}'";
		final Instant insertSignaturesStart = Instant.now();
		LOGGER.info(insertSignaturesFormat, "START", SIGNATURES);

		final SetMultimap<String, String> signatures = TreeMultimap.create();
		int signatureCount = 0;

		try (final PreparedStatement insertSignature = connection.prepareStatement(getInsertSignatureCmd())) {

			for (Person person : persons) {

				final String personKey = person.getKey();

				for (Publication publication : person.getPublications()) {

					final String recordKey = publication.getKey();

					if (signatures.put(personKey, recordKey)) {

						insertSignature.setString(1, personKey);
						insertSignature.setString(2, recordKey);
						insertSignature.setInt(3, publication.getYear());

						insertSignature.addBatch();

						if ((++signatureCount % BATCH_SIZE) == 0) {
							LOGGER.info("Processed {} signatures...", signatureCount);
							insertSignature.executeBatch();
						}
					}
				}
			}
			insertSignature.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertSignaturesFormat, "FAILED", SIGNATURES), e);
			System.exit(1);
		}
		LOGGER.info(insertSignaturesFormat + " (Duration: {})", "END", SIGNATURES,
				Duration.between(insertSignaturesStart, Instant.now()));
	}

	private static void wrapConferenceRecordsTo(final Connection connection) {

		reCreateTable(connection, CONFERENCE_RECORDS, getCreateConferenceRecordsCmd(), true);

		final String insertLogFormat = "{}: Batch-inserting conference records into table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", CONFERENCE_RECORDS);

		final SetMultimap<String, String> records = TreeMultimap.create();
		int recordCount = 0;

		try (final PreparedStatement insertRecord = connection.prepareStatement(getInsertRecordCmd());
				final PreparedStatement selectConferenceCites = connection
						.prepareStatement(getSelectConferenceCitesCmd())) {

			final ResultSet conferenceCites = selectConferenceCites.executeQuery();

			while (conferenceCites.next()) {

				final String citeKey = conferenceCites.getString("cite_key");
				final Publication citePublication = ExternalDataHub.getRecordDb().getPublication(citeKey);

				if (citePublication != null) {

					final TableOfContents toc = citePublication.getToc();

					if (toc != null) {

						for (Publication publication : toc.getPublications()) {

							final String recordKey = publication.getKey();

							if (records.put(citeKey, recordKey)) {

								insertRecord.setString(1, recordKey);
								insertRecord.setString(2, citeKey);
								insertRecord.setInt(3, publication.getYear());

								insertRecord.addBatch();

								if ((++recordCount % BATCH_SIZE) == 0) {
									LOGGER.info("Processed {} conference records... ", recordCount);
									insertRecord.executeBatch();
								}
							}
						}

					} else {
						if (records.put(citeKey, citeKey)) {

							insertRecord.setString(1, citeKey);
							insertRecord.setString(2, citeKey);
							insertRecord.setInt(3, citePublication.getYear());
							insertRecord.executeUpdate();
						}
					}

				} else {
					LOGGER.error("There is no publication for cite key '{}' in dblp. Skipping...", citeKey);
				}
			}
			insertRecord.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", CONFERENCE_RECORDS), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", CONFERENCE_RECORDS,
				Duration.between(insertStart, Instant.now()));
	}

	private static String getCreateConferenceRecordsCmd() {

		final String createConferenceRecordsCmd = String.format(

				"create table if not exists %s "

						+ "( %s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s varchar(255) not null, " + "%s int" + " )" + ";",

				CONFERENCE_RECORDS,

				GENERIC_ID,

				RECORD_KEY, CITE_KEY, PUBLICATION_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createConferenceRecordsCmd);
		return createConferenceRecordsCmd;
	}

	private static String getInsertRecordCmd() {

		final String insertRecordCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s" + " ) "

						+ "values (?, ?, ?)" + ";",

				CONFERENCE_RECORDS,

				RECORD_KEY, CITE_KEY, PUBLICATION_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertRecordCmd);
		return insertRecordCmd;
	}

	private static String getSelectConferenceCitesCmd() {

		final String selectConferenceCitesCmd = String.format(

				"select * from %s",

				CONFERENCE_CITES);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectConferenceCitesCmd);
		return selectConferenceCitesCmd;
	}

	private static String getCreateSignaturesCmd() {

		final String createSignaturesCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s varchar(255) not null, " + "%s int" + " ) " + ";",

				SIGNATURES,

				GENERIC_ID,

				PERSON_KEY, RECORD_KEY, PUBLICATION_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createSignaturesCmd);
		return createSignaturesCmd;
	}

	private static String getInsertSignatureCmd() {

		final String insertSignatureCmd = String.format(

				"insert into %s ( " + "%s, %s, %s " + ") " + "values (?, ?, ?)" + ";",

				SIGNATURES,

				PERSON_KEY, RECORD_KEY, PUBLICATION_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertSignatureCmd);
		return insertSignatureCmd;
	}

	private static String getMaterializeProceedingsViewCmd() {

		final String innerSelectProceedings = String.format(

				"select distinct streams.%s, cites.%s "

						+ "from %s streams "

						+ "join %s cites "

						+ "on (streams.%s = cites.%s) "

						+ "where %s is false and %s is false",

				STREAM_KEY, CITE_KEY,

				CONFERENCE_STREAMS,

				CONFERENCE_CITES,

				BHT_KEY, BHT_KEY,

				DEINDEXED, DISCONTINUED);

		final String joinWithEventYears = String.format(

				"select procs.*, hdblp.%s, hdblp.%s, hdblp.%s from (%s) procs "

						+ "left join %s hdblp "

						+ "on procs.%s = hdblp.%s",

				PUBLICATION_YEAR, EVENT_YEAR, C_DATE, innerSelectProceedings,

				HISTORICAL_RECORDS,

				CITE_KEY, RECORD_KEY);

		final String materializeProceedingsViewCmd = String.format(

				"create materialized view %s as %s" + ";",

				PROCEEDINGS_VIEW, joinWithEventYears);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", materializeProceedingsViewCmd);
		return materializeProceedingsViewCmd;
	}

	private static String getMaterializeTocViewCmd(final int lteYear) {

		final String innerSelectTocs = String.format(

				"select distinct procs.%s, procs.%s, records.%s, procs.%s, procs.%s "

						+ "from %s procs "

						+ "left join %s records "

						+ "on (procs.%s = records.%s)",

				STREAM_KEY, CITE_KEY, RECORD_KEY, PUBLICATION_YEAR, EVENT_YEAR,

				PROCEEDINGS_VIEW,

				CONFERENCE_RECORDS,

				CITE_KEY, CITE_KEY);

		final String filteredTocs = String.format(

				"select tocs.*, hdblp.%s from (%s) tocs "

						+ "join %s hdblp "

						+ "on tocs.%s = hdblp.%s ",

				C_DATE, innerSelectTocs,

				HISTORICAL_RECORDS,

				RECORD_KEY, RECORD_KEY);

		final String materializeTocViewCmd = String.format(

				"create materialized view %s as %s" + ";",

				TOC_VIEW, filteredTocs);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", materializeTocViewCmd);
		return materializeTocViewCmd;
	}

	private static String getMaterializeDetailViewCmd() {

		final String innerSelectDetailsCmd = String.format(

				"select distinct tocs.%s, tocs.%s, tocs.%s, signatures.%s, tocs.%s, tocs.%s, tocs.%s "

						+ "from %s tocs "

						+ "left join %s signatures "

						+ "on (tocs.%s = signatures.%s) "

						+ "where %s is not null",

				STREAM_KEY, CITE_KEY, RECORD_KEY, PERSON_KEY, PUBLICATION_YEAR, EVENT_YEAR, C_DATE,

				TOC_VIEW,

				SIGNATURES,

				RECORD_KEY, RECORD_KEY,

				PERSON_KEY);

		final String materializeDetailViewCmd = String.format(

				"create materialized view %s as %s" + ";",

				DETAIL_VIEW, innerSelectDetailsCmd);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", materializeDetailViewCmd);
		return materializeDetailViewCmd;
	}

	private static String getMaterializeProcRecordCountViewCmd(final int lteYear) {

		final String innerSelectDistinctRecords = String.format(

				"select distinct %s, %s, %s "

						+ "from %s "

						+ "where %s <= '%d-12-31'",

				STREAM_KEY, CITE_KEY, RECORD_KEY,

				TOC_VIEW,

				C_DATE, lteYear);

		final String selectRecordCountPerCite = String.format(

				"select drc.%s, drc.%s, count(distinct drc.%s) %s "

						+ "from (%s) drc "

						+ "group by drc.%s, drc.%s",

				STREAM_KEY, CITE_KEY, RECORD_KEY, RECORD_COUNT,

				innerSelectDistinctRecords,

				STREAM_KEY, CITE_KEY);

		final String joinWithProceedingsView = String.format(

				"select * from (%s) prc "

						+ "left join %s "

						+ "using (%s, %s)",

				selectRecordCountPerCite,

				PROCEEDINGS_VIEW,

				STREAM_KEY, CITE_KEY);

		final String materializeProcRecordCountViewCmd = String.format(

				"create materialized view %s as %s" + ";",

				PROCEEDINGS_RECORD_COUNT_VIEW, joinWithProceedingsView);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", materializeProcRecordCountViewCmd);
		return materializeProcRecordCountViewCmd;
	}

	private static String getMaterializeProcAuthorCountViewCmd(final int lteYear) {

		final String innerSelectDistinctPersons = String.format(

				"select distinct %s, %s, %s "

						+ "from %s "

						+ "where %s <= '%d-12-31'",

				STREAM_KEY, CITE_KEY, PERSON_KEY,

				DETAIL_VIEW,

				C_DATE, lteYear);

		final String selectAuthorCountPerCite = String.format(

				"select dac.%s, dac.%s, count(distinct dac.%s) %s "

						+ "from (%s) dac "

						+ "group by dac.%s, dac.%s",

				STREAM_KEY, CITE_KEY, PERSON_KEY, AUTHOR_COUNT,

				innerSelectDistinctPersons,

				STREAM_KEY, CITE_KEY);

		final String joinWithProceedingsView = String.format(

				"select * from (%s) pac "

						+ "left join %s "

						+ "using (%s, %s)",
				selectAuthorCountPerCite,

				PROCEEDINGS_VIEW,

				STREAM_KEY, CITE_KEY);

		final String materializeProcAuthorCountViewCmd = String.format(

				"create materialized view %s as %s" + ";",

				PROCEEDINGS_AUTHOR_COUNT_VIEW, joinWithProceedingsView);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", materializeProcAuthorCountViewCmd);
		return materializeProcAuthorCountViewCmd;
	}

	private static String getMaterializePersonRecordCountViewCmd(final int lteYear) {

		final String materializePersonRecordCountViewCmd = String.format(

				"create materialized view %s as "

						+ "select details.%s, details.%s, details.%s, count(distinct details.%s) %s "

						+ "from %s details "

						+ "where %s <= '%d-12-31' "

						+ "group by details.%s, details.%s, details.%s" + ";",

				PERSON_YEAR_RECORD_COUNT_VIEW,

				PERSON_KEY, PUBLICATION_YEAR, EVENT_YEAR, RECORD_KEY, RECORD_COUNT,

				DETAIL_VIEW,

				C_DATE, lteYear,

				PERSON_KEY, PUBLICATION_YEAR, EVENT_YEAR);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", materializePersonRecordCountViewCmd);
		return materializePersonRecordCountViewCmd;
	}

}
