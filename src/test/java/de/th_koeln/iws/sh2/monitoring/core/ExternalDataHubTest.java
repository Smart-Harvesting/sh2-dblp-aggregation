/**
 * 
 */
package de.th_koeln.iws.sh2.monitoring.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.NoSuchFileException;
import java.util.Map;

import org.dblp.bhtdb.BhtDb;
import org.dblp.citations.auxiliary.ReadonlyAuxCollection;
import org.dblp.mmdb.RecordDb;
import org.dblp.streamdb.StreamDb;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.th_koeln.iws.sh2.aggregation.core.ExternalDataHub;
import de.th_koeln.iws.sh2.aggregation.core.xml.DblpEntryProcessor;
import de.th_koeln.iws.sh2.aggregation.core.xml.data.DblpEntry;

/**
 * @author mandy
 *
 */
class ExternalDataHubTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link de.th_koeln.iws.sh2.aggregation.core.ExternalDataHub#getStreamDb()}.
	 */
	@Test
	void testGetStreamDb() {
		StreamDb streamDb = ExternalDataHub.getStreamDb();
		assertTrue(streamDb.flatNumberOfConferences()>0);
	}

	/**
	 * Test method for
	 * {@link de.th_koeln.iws.sh2.aggregation.core.ExternalDataHub#getBhtDb()}.
	 */
	@Test
	void testGetBhtDb() {
		BhtDb bhtDb = ExternalDataHub.getBhtDb();
		assertTrue(bhtDb.numberOfPages()>0);
	}

	/**
	 * Test method for
	 * {@link de.th_koeln.iws.sh2.aggregation.core.ExternalDataHub#getRecordDb()}.
	 */
	@Test
	void testGetRecordDb() {
		RecordDb recordDb = ExternalDataHub.getRecordDb();
		assertTrue(recordDb.numberOfTocs()>0);
	}

	/**
	 * Test method for
	 * {@link de.th_koeln.iws.sh2.aggregation.core.ExternalDataHub#getOagSingleReadOnlyHandle()}.
	 * @throws NoSuchFileException
	 */
	@Test
	void testGetOagSingleReadOnlyHandle() throws NoSuchFileException {
		ReadonlyAuxCollection oagSingleReadOnlyHandle = ExternalDataHub.getOagSingleReadOnlyHandle();
		String status = oagSingleReadOnlyHandle.status();
		assertFalse(status.isEmpty());
		assertFalse(status.length()>0);
	}

	/**
	 * Test method for
	 * {@link de.th_koeln.iws.sh2.aggregation.core.ExternalDataHub#getHdblpProcessor()}.
	 */
	@Test
	void testGetHdblpProcessor() {
		DblpEntryProcessor hdblpProcessor = ExternalDataHub.getHdblpProcessor();
		Map<String, DblpEntry> entries = hdblpProcessor.getEntries();
		assertFalse(entries.isEmpty());
		assertEquals(6760373, entries.size());
	}
}
