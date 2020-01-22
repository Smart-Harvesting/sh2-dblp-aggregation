package de.th_koeln.iws.sh2.aggregation.core;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dblp.bhtdb.BhtDb;
import org.dblp.citations.auxiliary.ReadonlyAuxCollection;
import org.dblp.mmdb.RecordDb;
import org.dblp.streamdb.StreamDb;
import org.xml.sax.SAXException;

import de.th_koeln.iws.sh2.aggregation.config.PropertiesUtil;
import de.th_koeln.iws.sh2.aggregation.core.xml.DblpEntryProcessor;
import de.th_koeln.iws.sh2.aggregation.core.xml.HdblpParser;

public class ExternalDataHub {

	private static final Logger LOGGER = LogManager.getLogger(ExternalDataHub.class);
	private static final Properties EXTERNAL_DATA_SETUP = PropertiesUtil.loadExternalDataSetupConfig();

	private static StreamDb STREAM_DB = null;
	private static BhtDb BHT_DB = null;
	private static RecordDb RECORD_DB = null;
	private static ReadonlyAuxCollection OAG_AUX_SINGLE_RO = null;
	private static DblpEntryProcessor HDBLP_PROCESSOR = null;
	private static String SIGN_AFFIL_LOCATIONS_CSV = null;

	public static StreamDb getStreamDb() {
		if (STREAM_DB == null) {
			LOGGER.info("START: Creating StreamDb");
			final Instant streamDbStart = Instant.now();
			try {
				STREAM_DB = new StreamDb(EXTERNAL_DATA_SETUP.getProperty("xml.streams"));
			} catch (IOException | XMLStreamException e) {
				LOGGER.fatal("Unable to create a singleton instance of the dblp StreamDb", e);
			}
			LOGGER.info("END: Creating StreamDb (Duration: " + Duration.between(streamDbStart, Instant.now()).toString()
					+ ")");
		}
		return STREAM_DB;
	}

	public static BhtDb getBhtDb() {
		if (BHT_DB == null) {
			LOGGER.info("START: Creating BhtDb");
			final Instant bhtDbStart = Instant.now();
			try {
				BHT_DB = new BhtDb(EXTERNAL_DATA_SETUP.getProperty("xml.bht"));
			} catch (IOException | XMLStreamException e) {
				LOGGER.fatal("Unable to create a singleton instance of the dblp BhtDb", e);
			}
			LOGGER.info(
					"END: Creating BhtDb (Duration: " + Duration.between(bhtDbStart, Instant.now()).toString() + ")");
		}
		return BHT_DB;
	}

	public static RecordDb getRecordDb() {
		if (RECORD_DB == null) {
			LOGGER.info("START: Creating RecordDb");
			final Instant recordDbStart = Instant.now();
			try {
				RECORD_DB = new RecordDb(EXTERNAL_DATA_SETUP.getProperty("xml.dblp"),
						EXTERNAL_DATA_SETUP.getProperty("dtd.dblp"), true);
			} catch (IOException | SAXException e) {
				LOGGER.fatal("Unable to create a singleton instance of the dblp RecordDb", e);
			}
			LOGGER.info("END: Creating RecordDb (Duration: " + Duration.between(recordDbStart, Instant.now()).toString()
					+ ")");
		}
		return RECORD_DB;
	}

	public static ReadonlyAuxCollection getOagSingleReadOnlyHandle() throws NoSuchFileException {
		if (OAG_AUX_SINGLE_RO == null) {
			final Path collectionDirectoryPath = Paths.get(EXTERNAL_DATA_SETUP.getProperty("path.oaga"));
			if (collectionDirectoryPath.toFile().exists() && collectionDirectoryPath.toFile().isDirectory()) {
				LOGGER.info("START: Creating handle for single read-only access to OAG auxiliary data");
				final Instant oagSingleStart = Instant.now();
				OAG_AUX_SINGLE_RO = new ReadonlyAuxCollection(collectionDirectoryPath.toString());
				LOGGER.info("END: Creating handle for single read-only access to OAG auxiliary data (Duration: "
						+ Duration.between(oagSingleStart, Instant.now()).toString() + ")");
			} else {
				throw new NoSuchFileException(collectionDirectoryPath.toString());
			}
		}
		return OAG_AUX_SINGLE_RO;
	}

	public static DblpEntryProcessor getHdblpProcessor() {
		if (HDBLP_PROCESSOR == null) {
			LOGGER.info("START: Processing historical dblp data");
			final Instant hdblpStart = Instant.now();
			HDBLP_PROCESSOR = new DblpEntryProcessor();
			HdblpParser parser = new HdblpParser(HDBLP_PROCESSOR, EXTERNAL_DATA_SETUP.getProperty("dtd.dblp"));
			parser.process(EXTERNAL_DATA_SETUP.getProperty("xml.hdblp"));
			LOGGER.info("END: Processing historical dblp data (Duration: "
					+ Duration.between(hdblpStart, Instant.now()).toString() + ")");
		}
		return HDBLP_PROCESSOR;
	}

	public static String getSignAffilLocationsCsv() {
		if (SIGN_AFFIL_LOCATIONS_CSV == null) {
			SIGN_AFFIL_LOCATIONS_CSV = EXTERNAL_DATA_SETUP.getProperty("csv.msa");
		}
		return SIGN_AFFIL_LOCATIONS_CSV;
	}

	public static void flushStreamDb() {
		STREAM_DB = null;
	}

	public static void flushBhtDb() {
		BHT_DB = null;
	}

	public static void flushRecordDb() {
		RECORD_DB = null;
	}

}
