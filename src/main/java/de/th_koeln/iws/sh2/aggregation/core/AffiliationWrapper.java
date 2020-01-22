package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSTITUTION_COUNTRY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSTITUTION_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSTITUTION_NORM_NAME;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSTITUTION_SURFACE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PERSON_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PERSON_SURFACE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SIGN_AFFIL_LOCATIONS;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

public class AffiliationWrapper extends CoreComponent {

	private static Logger LOGGER = LogManager.getLogger(AffiliationWrapper.class);
	private static final int BATCH_SIZE = 100000;

	public static void wrapTo(final Connection connection) throws SQLException {

		wrapToSignAffilLocations(connection);

	}

	private static void wrapToSignAffilLocations(final Connection connection) {

		reCreateTable(connection, SIGN_AFFIL_LOCATIONS, getCreateSignAffilLocationsCmd(), true);

		final String insertSignAffilLocationsFormat = "{}: Inserting signature-affiliation locations into table '{}'";
		final Instant insertSignAffilLocationsStart = Instant.now();
		LOGGER.info(insertSignAffilLocationsFormat, "START", SIGN_AFFIL_LOCATIONS);

		try (final PreparedStatement insertLocation = connection.prepareStatement(getInsertSignAffilLocationCmd());
				FileInputStream fis = new FileInputStream(ExternalDataHub.getSignAffilLocationsCsv());
				InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
				final CSVParser parser = new CSVParser(isr, CSVFormat.EXCEL.withHeader());) {

			for (final CSVRecord record : parser) {

				insertLocation.setString(1, record.get(PERSON_KEY).equals("null") ? null
						: String.format("homepages/%s", record.get(PERSON_KEY)));
				insertLocation.setString(2,
						record.get(PERSON_SURFACE).equals("null") ? null : record.get(PERSON_SURFACE));
				insertLocation.setString(3, record.get(RECORD_KEY).equals("null") ? null : record.get(RECORD_KEY));
				insertLocation.setInt(4, Integer
						.parseInt(record.get(PUBLICATION_YEAR).equals("null") ? "0" : record.get(PUBLICATION_YEAR)));
				insertLocation.setString(5,
						record.get(INSTITUTION_SURFACE).equals("null") ? null : record.get(INSTITUTION_SURFACE));
				insertLocation.setString(6,
						record.get(INSTITUTION_KEY).equals("null") ? null : record.get(INSTITUTION_KEY));
				insertLocation.setString(7,
						record.get(INSTITUTION_NORM_NAME).equals("null") ? null : record.get(INSTITUTION_NORM_NAME));
				insertLocation.setString(8,
						record.get(INSTITUTION_COUNTRY).equals("null") ? null : record.get(INSTITUTION_COUNTRY));

				insertLocation.addBatch();

				if ((record.getRecordNumber() % BATCH_SIZE) == 0) {
					LOGGER.info("Processed {} locations...", record.getRecordNumber());
					insertLocation.executeBatch();
				}
			}
			insertLocation.executeBatch();

		} catch (Exception e) {
			LOGGER.fatal(new ParameterizedMessage(insertSignAffilLocationsFormat, "FAILED", SIGN_AFFIL_LOCATIONS), e);
			System.exit(1);
		}
		LOGGER.info(insertSignAffilLocationsFormat + " (Duration: {})", "END", SIGN_AFFIL_LOCATIONS,
				Duration.between(insertSignAffilLocationsStart, Instant.now()));
	}

	private static String getCreateSignAffilLocationsCmd() {

		final String createSignAffilLocationsCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255), " + "%s varchar(255), "

						+ "%s varchar(255), " + "%s int, "

						+ "%s varchar(4000), " + "%s varchar(255), " + "%s varchar(255), " + "%s varchar(255)" + ")"
						+ ";",

				SIGN_AFFIL_LOCATIONS,

				GENERIC_ID,

				PERSON_KEY, PERSON_SURFACE,

				RECORD_KEY, PUBLICATION_YEAR,

				INSTITUTION_SURFACE, INSTITUTION_KEY, INSTITUTION_NORM_NAME, INSTITUTION_COUNTRY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createSignAffilLocationsCmd);
		return createSignAffilLocationsCmd;
	}

	private static String getInsertSignAffilLocationCmd() {

		final String insertSignAffilLocationCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s, %s, %s, %s, %s, %s, %s" + " ) "

						+ "values (?, ?, ?, ?, ?, ?, ?, ?)" + ";",

				SIGN_AFFIL_LOCATIONS,

				PERSON_KEY, PERSON_SURFACE,

				RECORD_KEY, PUBLICATION_YEAR,

				INSTITUTION_SURFACE, INSTITUTION_KEY, INSTITUTION_NORM_NAME, INSTITUTION_COUNTRY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertSignAffilLocationCmd);
		return insertSignAffilLocationCmd;
	}

}
