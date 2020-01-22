package de.th_koeln.iws.sh2.monitoring.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimap;

import de.th_koeln.iws.sh2.aggregation.core.DbDataReader;
import de.th_koeln.iws.sh2.aggregation.db.DatabaseManager;
import de.th_koeln.iws.sh2.aggregation.model.ConferenceStream;
import de.th_koeln.iws.sh2.aggregation.model.StreamRecord;


class DbDataReaderTest {

	private static DbDataReader reader;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		reader = new DbDataReader(DatabaseManager.getInstance());
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testGetStreams() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());
		assertEquals(4395, setOfStreams.size());

		List<ConferenceStream> listOfStreams = reader.getAsListOfStreams();
		System.out.println("Number of conference streams in list: " + listOfStreams.size());
		assertEquals(4395, listOfStreams.size());

		assertEquals(setOfStreams.size(), listOfStreams.size(), "Set and List size should be same");

		Multimap<String, StreamRecord> multiMap = reader.getAsMultiMap();
		System.out.println("Number of mappings from streams to stream records: " + multiMap.size());
		System.out.println("Number of distinct mappings from streams to stream records: " + multiMap.keySet().size());
		assertEquals(4395, multiMap.keySet().size());

		assertEquals(multiMap.keySet().size(), listOfStreams.size(),
				"Number of distinct entries should equal list size");
	}

	@Test
	void testGetStreamRatings() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithRating = setOfStreams.stream().filter(c -> c.getAvgRating() != null).count();
		System.out.println("Number of non-null ratings: " + streamsWithRating);

		// make sure that non-null means also some value > 0
		long streamsWithPositiveRating = setOfStreams.stream()
				.filter(c -> (c.getAvgRating() != null) && (c.getAvgRating() > 0.0)).count();
		System.out.println("Number of non-0 ratings: " + streamsWithPositiveRating);
	}

	/*
	 * TODO for the future: value of score should be null if not available
	 */
	@Test
	void testGetIntlScores() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithIntl = setOfStreams.stream().filter(c -> c.getIntlScore() != null).count();
		System.out.println("Number of non-null intl scores: " + streamsWithIntl);

		long streamsWithPositiveIntl = setOfStreams.stream().filter(c -> c.getIntlScore() > 0.0).count();
		System.out.println("Number of non-0 intl scores: " + streamsWithPositiveIntl);
	}

	@Test
	void testGetPromScores() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithProm = setOfStreams.stream().filter(c -> c.getProminence() != null).count();
		System.out.println("Number of non-null prom scores: " + streamsWithProm);

		long streamsWithPositiveProm = setOfStreams.stream().filter(c -> c.getProminence() > 0.0).count();
		System.out.println("Number of non-0 prom scores: " + streamsWithPositiveProm);
	}

	@Test
	void testGetCiteScores() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithCitation = setOfStreams.stream().filter(c -> c.getCitationsScore() != null).count();
		System.out.println("Number of non-null cite scores: " + streamsWithCitation);

		long streamsWithPositiveCitation = setOfStreams.stream().filter(c -> c.getCitationsScore() > 0.0).count();
		System.out.println("Number of non-0 cite scores: " + streamsWithPositiveCitation);
	}

	@Test
	void testGetAffilScores() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithAffiliations = setOfStreams.stream().filter(c -> c.getAffilScore() != null).count();
		System.out.println("Number of non-null affil scores: " + streamsWithAffiliations);

		long streamsWithPositiveAffiliations = setOfStreams.stream()
				.filter(c -> (c.getAffilScore() != null) && (c.getAffilScore() > 0.0)).count();
		System.out.println("Number of non-0 affil scores: " + streamsWithPositiveAffiliations);
	}

	@Test
	void testGetSizeScores() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithSize = setOfStreams.stream().filter(c -> c.getAvgSize() != null).count();
		System.out.println("Number of non-null size scores: " + streamsWithSize);

		long streamsWithPositiveSize = setOfStreams.stream().filter(c -> c.getAvgSize() > 0.0).count();
		System.out.println("Number of non-0 size scores: " + streamsWithPositiveSize);
	}

	@Test
	void testGetLogScores() {
		Set<ConferenceStream> setOfStreams = Collections.unmodifiableSet(reader.getAsSetOfStreams());
		System.out.println("Number of conference streams in set: " + setOfStreams.size());

		long streamsWithSize = setOfStreams.stream().filter(c -> c.getLogScores() != null).count();
		System.out.println("Number of non-null size scores: " + streamsWithSize);

		int i =0;
		for (ConferenceStream c : setOfStreams) {
			List<Double> collect = c.getLogScores().values().stream().filter(s -> (s!=null) && (s>0.0)).collect(Collectors.toList());
			if(!collect.isEmpty())
				i++;
		}

		System.out.println("Number of conferences with at least 1 log score: " + i);
	}

}
