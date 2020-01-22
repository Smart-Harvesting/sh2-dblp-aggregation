package de.th_koeln.iws.sh2.aggregation.model.util;

import java.time.Month;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.math.Quantiles;

import de.th_koeln.iws.sh2.aggregation.model.StreamRecord;


public class Calculator {

	private static Logger LOGGER = LogManager.getLogger(Calculator.class);

	/**
	 * Calculate the interval between events of a list of conference records.
	 * 
	 * @param eventRecords
	 *            the list of records whose interval should be found. Assumed to be
	 *            sorted in descending order.
	 * @param sampleSize
	 *            use only this many entries of records
	 * @return the estimated interval
	 */
	public static long calcMedianInterval(Multimap<YearMonth, StreamRecord> eventRecords, int sampleSize) {
		final List<Long> eventMonthDeltas = Lists.newArrayList();

		ImmutableList<YearMonth> mostRecentYearMonths = eventRecords.keySet().stream().limit(sampleSize + 1)
				.collect(ImmutableList.toImmutableList());

		for (int i = 0; i < mostRecentYearMonths.size(); i++) {

			if (i > 0) {
				final long monthsBetween = mostRecentYearMonths.get(i).until(mostRecentYearMonths.get(i - 1),
						ChronoUnit.MONTHS);
				eventMonthDeltas.add(monthsBetween);
			}
		}

		LOGGER.debug("eventMonthDeltas: {}", eventMonthDeltas);
		if (eventMonthDeltas.size() > 0) {
			long median = (long) Math.floor(Quantiles.median().compute(eventMonthDeltas));
			LOGGER.debug("median: {}", median);
			return median;
		} else {
			return 12;
		}
	}

	/**
	 * Determine the "usual" (=mode) event month of a list of conference records.
	 * 
	 * @param eventRecords
	 *            the list of records whose most frequent event month should be
	 *            found.
	 * @param sampleSize
	 *            use only this many entries of records
	 * @return the estimated usual event month
	 */
	public static Month calcModeMonth(Multimap<YearMonth, StreamRecord> eventRecords, int sampleSize) {
		final ImmutableMultiset<Month> mostRecentMonths = eventRecords.keySet().stream().limit(sampleSize)
				.map(ym -> ym.getMonth()).collect(ImmutableMultiset.toImmutableMultiset());

		final int maxMonthCount = mostRecentMonths.elementSet().stream().map(e -> mostRecentMonths.count(e))
				.max(Comparator.naturalOrder()).get();

		LOGGER.debug("most recent months (sample size: {}): {}", sampleSize, mostRecentMonths);

		final ImmutableSet<Month> modes = mostRecentMonths.stream()
				.filter(monthValue -> mostRecentMonths.count(monthValue) == maxMonthCount)
				.collect(ImmutableSet.toImmutableSet());

		LOGGER.debug("modes: {}", modes);

		if (modes.size() == 1) {
			return modes.asList().get(0);
		} else {
			return eventRecords.keySet().stream().map(ym -> ym.getMonth())
					.filter(recentYearMonth -> mostRecentMonths.count(recentYearMonth) == maxMonthCount)
					.sorted(Comparator.reverseOrder()).findFirst().get();
		}
	}

	// kudos to stackoverflow {@link https://stackoverflow.com/a/4191729/1948454}
	// TODO mal überlegen, ob diese Methode bleiben kann (würde dann innerhalb von
	// calcModeMonth aufgerufen werden), oder ob die momentane oben besser ist weil
	// dort multimodale Verteilungen besser gehandhabt werden?
	public static List<Month> getModes(final Stream<Month> numbers) {
		final Map<Month, Long> countFrequencies = numbers
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		final long maxFrequency = countFrequencies.values().stream().mapToLong(count -> count).max().orElse(-1);

		return countFrequencies.entrySet().stream().filter(tuple -> tuple.getValue() == maxFrequency)
				.map(Map.Entry::getKey).collect(Collectors.toList());
	}

	/**
	 * Calculates the median of insert delays for a list of conference records.
	 * 
	 * The parameter list is expected to be already sorted by event year in
	 * descending order (latest event in 1st position). It is also expected to
	 * contain at most 5 entries.
	 * 
	 * @param eventRecords
	 *            the list of records whose median insert delay should be
	 *            determined. Assumed to be sorted in descending order.
	 * @param sampleSize
	 *            use only this many entries of records
	 * @return the median of insertion delays
	 */
	public static int calcMedianDelay(Multimap<YearMonth, StreamRecord> eventRecords, int sampleSize) {
		List<Long> delays = eventRecords.keySet().stream().limit(sampleSize).map(ym -> eventRecords.get(ym)).flatMap(Collection::stream).map(r -> r.getInsertDelayInMonths())
				.collect(Collectors.toList());

		LOGGER.debug("delays: {}", delays);
		int median = (int) Math.floor(Quantiles.median().compute(delays));
		LOGGER.debug("median delay: {}", median);
		return median;
	}

	/**
	 * Calculate how many months have passed since the latest record of the latest
	 * event has been added to the dataset.
	 * 
	 * @param conf
	 *            the conference
	 * @param ym
	 *            the reference date as year-month
	 * @return if there exist records created before ym, the number of months that
	 *         have passed since the creation of the latest record of the latest
	 *         event.
	 */
	public static Optional<Long> calcMonthsSinceLastCreation(TreeMultimap<YearMonth, StreamRecord> records,
			YearMonth ym) {
		// no records = no delay
		if (records.isEmpty()) {
			return Optional.empty();
		}

		// this list is already sorted by event year in descending order, so first entry
		// is latest event
		YearMonth latestEventYM = records.keySet().first();
		NavigableSet<StreamRecord> latestEventRecords = records.get(latestEventYM);

		// first record of latest event is the one that has been added more recently
		StreamRecord latestEventRecord = latestEventRecords.first();

		YearMonth creationYM = YearMonth.from(latestEventRecord.getcDate());

		return Optional.of(creationYM.until(ym, ChronoUnit.MONTHS));
	}
}
