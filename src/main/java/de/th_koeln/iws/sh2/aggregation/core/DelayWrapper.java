package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_MONTH;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.EVENT_YEAR;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RAW_DELAY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_DELAYS;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.PROCEEDINGS_VIEW;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeBasedTable;
import com.google.common.math.Quantiles;

public class DelayWrapper extends CoreComponent {

	private static final double DEFAULT_EVENT_DELTA = 12.0;

	private static Logger LOGGER = LogManager.getLogger(DelayWrapper.class);

	private static final int DELTA_MEDIAN_SAMPLE_SIZE = 5;

	private static final int BATCH_SIZE = 1000;

	public static void wrapTo(final Connection connection, final int evaluationYear) {

		// streamKey, citeKey, creationYearMonth
		final TreeBasedTable<String, String, YearMonth> creationYearMonths = TreeBasedTable
				.create(Comparator.naturalOrder(), Comparator.reverseOrder());
		// streamKey, eventYearMonth, set of citeKeys associated with eventYearMonth
		final TreeBasedTable<String, YearMonth, Set<String>> eventYearMonths = TreeBasedTable
				.create(Comparator.naturalOrder(), Comparator.reverseOrder());

		/*
		 * fill the two tables above with dblp data
		 */
		getRawTimeData(connection, creationYearMonths, eventYearMonths);

		/*
		 * streamKey, evaluationYearMonth, median distance in years between up to 6
		 * latest events, i.e. delta_{month}(c) according to the paper
		 */
		final TreeBasedTable<String, YearMonth, Double> eventDeltas = TreeBasedTable.create(Comparator.naturalOrder(),
				Comparator.naturalOrder());

		/*
		 * streamKey, evaluationYearMonth, median distance in months between
		 * eventYearMonth and creationYearMonth for up to 5 latest events, i.e.
		 * delta_{month}(c) according to the paper
		 */
		final TreeBasedTable<String, YearMonth, Double> referenceDeltasInMonths = TreeBasedTable
				.create(Comparator.naturalOrder(), Comparator.naturalOrder());
		/*
		 * streamKey, evaluationYearMonth, constructed most recent event year-month (
		 * (m(c), year(d_n(c)) according to the paper
		 */
		final TreeBasedTable<String, YearMonth, YearMonth> constructedMostRecentEvents = TreeBasedTable
				.create(Comparator.naturalOrder(), Comparator.naturalOrder());

		final TreeBasedTable<String, YearMonth, Long> rawDelays = TreeBasedTable.create(Comparator.naturalOrder(),
				Comparator.naturalOrder());

		// iterate over all months of the evaluation year, for all streams
		for (final YearMonth evaluationYearMonth : Sets.newHashSet(Month.values()).stream()
				.map(month -> YearMonth.of(evaluationYear, month))
				.collect(ImmutableSortedSet.toImmutableSortedSet(Comparator.naturalOrder()))) {

			for (final String streamKey : eventYearMonths.rowKeySet()) {

				LOGGER.debug("[{}, {}] event-to-cites map: {}", evaluationYearMonth, streamKey,
						eventYearMonths.row(streamKey));

				// list of $$ n = DELTA_MEDIAN_SAMPLE_SIZE + 1 $$ most recent event year-months
				// or less
				final ImmutableList<YearMonth> mostRecentYearMonths = eventYearMonths.row(streamKey).keySet().stream()
						.filter(eventYearMonth -> eventYearMonths.get(streamKey, eventYearMonth).stream()
								.filter(citeKey -> creationYearMonths.get(streamKey, citeKey)
										.isBefore(evaluationYearMonth))
								.findFirst().isPresent())
						.limit(DELTA_MEDIAN_SAMPLE_SIZE + 1).collect(ImmutableList.toImmutableList());
				LOGGER.debug("[{}, {}] event list: {}", evaluationYearMonth, streamKey, mostRecentYearMonths);

				if (mostRecentYearMonths.size() > 0) {
					/*
					 * fill the distance-related tables above with data for raw-delay calculation
					 */
					setDeltaValues(mostRecentYearMonths, eventYearMonths, creationYearMonths, eventDeltas,
							referenceDeltasInMonths, streamKey, evaluationYearMonth);

					/*
					 * fill the remaining table with constructed most recent event year-months:
					 * 
					 * (m(c), year(d_n(c))
					 */
					setConstructedMostRecentEvent(mostRecentYearMonths, constructedMostRecentEvents, streamKey,
							evaluationYearMonth);

					if (constructedMostRecentEvents.get(streamKey, evaluationYearMonth) != null) {
						/*
						 * add the median distance in years between most recent event years to the
						 * constructed most recent event: (m(c), year(d_n(c)) + delta_{year}(c)
						 */
						final YearMonth expectedNextEvent = constructedMostRecentEvents
								.get(streamKey, evaluationYearMonth)
								.plusMonths(Double.valueOf(Math.floor(eventDeltas.get(streamKey, evaluationYearMonth)))
										.longValue());
						LOGGER.debug("[{}, {}] expected next event: {}", evaluationYearMonth, streamKey,
								expectedNextEvent);

						/*
						 * add the median distance in months between the event year and the creation
						 * date for the most recent events:
						 * 
						 * d_{n+1}(c) = (m(c), year(d_n(c)) + delta_{year}(c) + delta_{month}(c)
						 */
						final YearMonth expectedNextEntry = getExpectedNextEntry(expectedNextEvent,
								referenceDeltasInMonths, streamKey, evaluationYearMonth);
						LOGGER.debug("[{}, {}] expected next entry: {}", evaluationYearMonth, streamKey,
								expectedNextEntry);

						/*
						 * Compute the raw delay:
						 * 
						 * \Delta(c) = \now - d_{n+1}$
						 */
						final long rawDelay = expectedNextEntry.until(evaluationYearMonth, ChronoUnit.MONTHS);
						LOGGER.debug("[{}, {}] raw delay: {}", evaluationYearMonth, streamKey, rawDelay);
						rawDelays.put(streamKey, evaluationYearMonth, rawDelay);
					}
				}
			}
		}

		final ImmutableSortedSet<String> streamKeys = getStreamKeys(connection);

		prepareRawValueTable(connection, RAW_DELAYS, streamKeys, evaluationYear);

		final String updateLogFormat = "{}: Batch-updating raw log scores in table '{}'";
		final Instant updateStart = Instant.now();
		LOGGER.info(updateLogFormat, "START", RAW_DELAYS);

		for (final YearMonth evaluationYearMonth : rawDelays.columnKeySet()) {

			try (final PreparedStatement updateRawDelays = connection
					.prepareStatement(getUpdateRawValueCmd(RAW_DELAYS, getColumnName(evaluationYearMonth)))) {

				int streamCount = 0;

				for (final String streamKey : rawDelays.rowKeySet()) {

					if (rawDelays.get(streamKey, evaluationYearMonth) != null) {

						updateRawDelays.setDouble(1, rawDelays.get(streamKey, evaluationYearMonth));
						updateRawDelays.setString(2, streamKey);

						updateRawDelays.addBatch();

						if ((++streamCount % BATCH_SIZE) == 0) {
							LOGGER.info("Processed {} streams...", streamCount);
							updateRawDelays.executeBatch();
						}
					}
				}
				updateRawDelays.executeBatch();

			} catch (SQLException e) {
				LOGGER.fatal(new ParameterizedMessage(updateLogFormat, "FAILED", RAW_DELAYS), e);
				System.exit(1);
			}
			LOGGER.info(updateLogFormat + " (Duration: {})", "END", RAW_DELAYS,
					Duration.between(updateStart, Instant.now()));
		}
	}

	// private static void setRawDelays(final TreeBasedTable<String, YearMonth,
	// YearMonth> constructedMostRecentEvents,
	// final TreeBasedTable<String, YearMonth, Double> eventDeltas,
	// final TreeBasedTable<String, YearMonth, Double> referenceDeltasInMonths,
	// final TreeBasedTable<String, YearMonth, Long> rawDelays, final YearMonth
	// evaluationYearMonth) {
	// /*
	// * TODO: needs clean-up, should not occur due to better filtering of most
	// recent
	// * events according to evaluationYearMonth final double
	// *
	// * medianAllDeltaReferenceMonths =
	// * Quantiles.median().compute(referenceDeltasInMonths.values());
	// *
	// */
	// for (final String streamKey : constructedMostRecentEvents.rowKeySet()) {
	//
	// LOGGER.debug("[{}, {}] constructed last event: {}", evaluationYearMonth,
	// streamKey,
	// constructedMostRecentEvents.get(streamKey, evaluationYearMonth));
	//
	// }
	// }

	private static void getRawTimeData(final Connection connection,
			final TreeBasedTable<String, String, YearMonth> creationYearMonths,
			final TreeBasedTable<String, YearMonth, Set<String>> eventYearMonths) {
		final String aggregateLogFormat = "{}: Aggregating raw values from view '{}' and table '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", PROCEEDINGS_VIEW, HISTORICAL_RECORDS);

		try (final PreparedStatement aggregateRawDelayInfo = connection.prepareStatement(getSelectRawDelayInfoCmd())) {

			final ResultSet rawDelayInfoResults = aggregateRawDelayInfo.executeQuery();

			while (rawDelayInfoResults.next()) {

				final String streamKey = rawDelayInfoResults.getString(STREAM_KEY);
				final String citeKey = rawDelayInfoResults.getString(CITE_KEY);

				final LocalDate creationDate = rawDelayInfoResults.getDate(C_DATE).toLocalDate();
				final YearMonth creationYearMonth = YearMonth.of(creationDate.getYear(), creationDate.getMonth());

				creationYearMonths.put(streamKey, citeKey, creationYearMonth);

				final int eventYear = rawDelayInfoResults.getInt(EVENT_YEAR);
				final int eventMonth = rawDelayInfoResults.getInt(EVENT_MONTH);
				final YearMonth eventYearMonth = YearMonth.of(eventYear, eventMonth);

				if (eventYearMonths.get(streamKey, eventYearMonth) == null) {
					eventYearMonths.put(streamKey, eventYearMonth, Sets.newHashSet());
				}
				eventYearMonths.get(streamKey, eventYearMonth).add(citeKey);
			}

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", PROCEEDINGS_VIEW, HISTORICAL_RECORDS),
					e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", PROCEEDINGS_VIEW, HISTORICAL_RECORDS,
				Duration.between(aggregateStart, Instant.now()));
	}

	private static void setDeltaValues(final ImmutableList<YearMonth> mostRecentYearMonths,
			final TreeBasedTable<String, YearMonth, Set<String>> eventYearMonths,
			final TreeBasedTable<String, String, YearMonth> creationYearMonths,
			final TreeBasedTable<String, YearMonth, Double> eventDeltas,
			final TreeBasedTable<String, YearMonth, Double> referenceDeltasInMonths, final String streamKey,
			final YearMonth evaluationYearMonth) {

		final List<Long> citeEventDeltas = Lists.newArrayList();
		final List<Long> citeReferenceDeltasInMonths = Lists.newArrayList();

		collectCiteDeltas(eventYearMonths, mostRecentYearMonths, citeEventDeltas, creationYearMonths,
				citeReferenceDeltasInMonths, streamKey, evaluationYearMonth);

		setEventDelta(eventDeltas, citeEventDeltas, streamKey, evaluationYearMonth);
		setReferenceDeltaInMonths(referenceDeltasInMonths, citeReferenceDeltasInMonths, streamKey, evaluationYearMonth);
	}

	private static void collectCiteDeltas(final TreeBasedTable<String, YearMonth, Set<String>> eventYearMonths,
			final ImmutableList<YearMonth> mostRecentYearMonths, final List<Long> citeEventDeltas,
			final TreeBasedTable<String, String, YearMonth> creationYearMonths,
			final List<Long> citeReferenceDeltasInMonths, final String streamKey, final YearMonth evaluationYearMonth) {
		for (int i = 0; i < mostRecentYearMonths.size(); i++) {

			if (i > 0) {
				final long citeEventDelta = mostRecentYearMonths.get(i).until(mostRecentYearMonths.get(i - 1),
						ChronoUnit.MONTHS);
				citeEventDeltas.add(citeEventDelta);
			}
			if (i < DELTA_MEDIAN_SAMPLE_SIZE) {

				for (final String citeKey : eventYearMonths.get(streamKey, mostRecentYearMonths.get(i))) {

					final YearMonth creationYearMonth = creationYearMonths.get(streamKey, citeKey);

					if ((creationYearMonth != null) && creationYearMonth.isBefore(evaluationYearMonth)) {
						LOGGER.debug("[{}, {}] valid cite: {}", evaluationYearMonth, streamKey, citeKey);
						final long monthsBetween = mostRecentYearMonths.get(i).until(creationYearMonth,
								ChronoUnit.MONTHS);
						citeReferenceDeltasInMonths.add(monthsBetween);
					} else {
						LOGGER.debug("[{}, {}] invalid cite: {}", evaluationYearMonth, streamKey, citeKey);
					}
				}
			}
		}
	}

	private static void setReferenceDeltaInMonths(
			final TreeBasedTable<String, YearMonth, Double> referenceDeltasInMonths,
			final List<Long> citeReferenceDeltasInMonths, final String streamKey, final YearMonth evaluationYearMonth) {
		if (citeReferenceDeltasInMonths.size() > 0) {
			LOGGER.debug("[{}, {}] cite-reference delta in months: {}", evaluationYearMonth, streamKey,
					citeReferenceDeltasInMonths);
			final double medianReferenceDelta = Quantiles.median().compute(citeReferenceDeltasInMonths);
			LOGGER.debug("[{}, {}] cite-reference-delta median: {}", evaluationYearMonth, streamKey,
					medianReferenceDelta);
			referenceDeltasInMonths.put(streamKey, evaluationYearMonth, medianReferenceDelta);
		}
	}

	private static void setEventDelta(final TreeBasedTable<String, YearMonth, Double> eventDeltas,
			final List<Long> citeEventDeltas, final String streamKey, final YearMonth evaluationYearMonth) {
		if (citeEventDeltas.size() > 0) {
			LOGGER.debug("[{}, {}] event deltas: {}", evaluationYearMonth, streamKey, citeEventDeltas);
			final double median = Quantiles.median().compute(citeEventDeltas);
			LOGGER.debug("[{}, {}] event-delta median: {}", evaluationYearMonth, streamKey, median);
			eventDeltas.put(streamKey, evaluationYearMonth, median);
		} else {
			LOGGER.debug("[{}, {}] default event delta: {}", evaluationYearMonth, streamKey, DEFAULT_EVENT_DELTA);
			eventDeltas.put(streamKey, evaluationYearMonth, DEFAULT_EVENT_DELTA);
		}
	}

	private static void setConstructedMostRecentEvent(final ImmutableList<YearMonth> mostRecentYearMonths,
			final TreeBasedTable<String, YearMonth, YearMonth> constructedMostRecentEvents, final String streamKey,
			final YearMonth evaluationYearMonth) {

		// multiset of month values occurring in the list above, further limited to 5
		final ImmutableMultiset<Integer> mostRecentMonths = mostRecentYearMonths.stream().limit(5)
				.map(eventYearMonth -> eventYearMonth.getMonthValue()).collect(ImmutableMultiset.toImmutableMultiset());

		if (mostRecentMonths.size() > 0) {

			final int maxMonthCount = mostRecentMonths.elementSet().stream().map(e -> mostRecentMonths.count(e))
					.max(Comparator.naturalOrder()).get();

			final ImmutableSet<Integer> modes = mostRecentMonths.stream()
					.filter(monthValue -> mostRecentMonths.count(monthValue) == maxMonthCount)
					.collect(ImmutableSet.toImmutableSet());

			final YearMonth constructedKnownEvent;

			if (modes.size() == 1) {
				constructedKnownEvent = YearMonth.of(mostRecentYearMonths.get(0).getYear(), modes.asList().get(0));
				LOGGER.debug("[{}, {}] constructed last event (mode): {}", evaluationYearMonth, streamKey,
						constructedKnownEvent);
			} else {
				LOGGER.debug("[{}, {}] constructed-last-event modes: {}", evaluationYearMonth, streamKey, modes);
				constructedKnownEvent = YearMonth.of(mostRecentYearMonths.get(0).getYear(),
						mostRecentYearMonths.stream()
						.filter(recentYearMonth -> mostRecentMonths
								.count(recentYearMonth.getMonthValue()) == maxMonthCount)
						.sorted(Comparator.reverseOrder()).findFirst().get().getMonthValue());
				LOGGER.debug("[{}, {}] constructed last event (most recent mode): {}", evaluationYearMonth, streamKey,
						constructedKnownEvent);
			}
			constructedMostRecentEvents.put(streamKey, evaluationYearMonth, constructedKnownEvent);
		}
	}

	private static YearMonth getExpectedNextEntry(final YearMonth expectedNextEvent,
			final TreeBasedTable<String, YearMonth, Double> referenceDeltasInMonths, final String streamKey,
			final YearMonth evaluationYearMonth) {
		if (referenceDeltasInMonths.get(streamKey, evaluationYearMonth) != null) {
			return expectedNextEvent.plusMonths(Double
					.valueOf(Math.floor(referenceDeltasInMonths.get(streamKey, evaluationYearMonth))).longValue());
		} else {
			/*
			 * TODO: needs clean-up, should not occur due to better filtering of most recent
			 * events according to evaluationYearMonth
			 *
			 * LOGGER.fatal( "[{}, {}] unknown last cite-reference-delta median; " +
			 * "falling back to overall reference-delta median: {}", evaluationYearMonth,
			 * streamKey, medianReferenceDeltaInMonths); expectedNextEntry =
			 * expectedNextEvent
			 * .plusMonths(Double.valueOf(Math.floor(medianReferenceDeltaInMonths)).
			 * longValue());
			 * 
			 */
			return null;
		}
	}

	private static void prepareRawValueTable(final Connection connection, final String tableName,
			final ImmutableSortedSet<String> streamKeys, final int evalYear) {
		reCreateTable(connection, tableName, getCreateRawValueTableCmd(tableName));

		insertStreamKeys(connection, tableName, streamKeys);

		for (final YearMonth yearMonth : Sets.newHashSet(Month.values()).stream()
				.map(month -> YearMonth.of(evalYear, month))
				.collect(ImmutableSortedSet.toImmutableSortedSet(Comparator.naturalOrder()))) {

			final String columnName = getColumnName(yearMonth);
			addMonthColumn(connection, tableName, columnName);

		}
	}

	private static void insertStreamKeys(final Connection connection, final String tableName,
			final ImmutableSortedSet<String> streamKeys) {
		final String insertLogFormat = "{}: Batch-inserting stream keys in table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", tableName);

		try (final PreparedStatement insertStreamKey = connection.prepareStatement(getInsertStreamKeyCmd(tableName))) {
			for (final String streamKey : streamKeys) {
				insertStreamKey.setString(1, streamKey);
				insertStreamKey.addBatch();
			}
			insertStreamKey.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", tableName), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", tableName,
				Duration.between(insertStart, Instant.now()));
	}

	private static String getColumnName(final YearMonth yearMonth) {
		return String.format("%s_y%dm%02d", RAW_DELAY, yearMonth.getYear(), yearMonth.getMonthValue());
	}

	private static void addMonthColumn(final Connection connection, final String tableName, final String columnName) {
		final String updateLogFormat = "{}: Batch-updating raw delays in table '{}'";
		final Instant updateStart = Instant.now();
		LOGGER.info(updateLogFormat, "START", tableName);

		try (final PreparedStatement addLogScoreColumn = connection
				.prepareStatement(getAddRawValueColumnCmd(tableName, columnName))) {

			addLogScoreColumn.executeUpdate();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(updateLogFormat, "FAILED", tableName), e);
			System.exit(1);
		}
		LOGGER.info(updateLogFormat + " (Duration: {})", "END", tableName,
				Duration.between(updateStart, Instant.now()));
	}

	private static String getInsertStreamKeyCmd(final String tableName) {

		final String insertStreamKeyCmd = String.format(

				"insert into %s "

						+ "( " + "%s" + " ) "

						+ "values (?)" + ";",

						tableName, STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertStreamKeyCmd);
		return insertStreamKeyCmd;
	}

	private static String getAddRawValueColumnCmd(final String tableName, final String columnName) {
		final String addLogScoreColumnCmd = String.format(

				"alter table %s "

						+ "add column if not exists %s bigint "

						+ "default -3333" + ";",

						tableName,

						columnName);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", addLogScoreColumnCmd);
		return addLogScoreColumnCmd;
	}

	private static String getSelectRawDelayInfoCmd() {
		final String selectRawDelayInfoCmd = String.format(

				"select distinct proc.stream_key, proc.cite_key, "

						+ "hist.c_date,hist.event_year, hist.event_month "

						+ "from public.dblp_conference_proceedings_view proc "

						+ "left join dblp_historical_records hist "

						+ "on proc.cite_key = hist.record_key "

						+ "where hist.c_date is not null "

						+ "and hist.event_year is not null "

						+ "and hist.event_month is not null" + ";");

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectRawDelayInfoCmd);
		return selectRawDelayInfoCmd;
	}

	private static String getCreateRawValueTableCmd(final String tableName) {
		final String createRawDelaysCmd = String.format(

				"create table if not exists %s " + "( "

						+ "%s bigserial primary key" + ", "

						+ "%s varchar(255) not null" + " )" + ";",

						tableName,

						GENERIC_ID,

						STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createRawDelaysCmd);
		return createRawDelaysCmd;
	}

	private static String getUpdateRawValueCmd(final String tableName, final String columnName) {
		final String updateLogScoresCmd = String.format(

				"update %s "

						+ "set %s = ? "

						+ "where %s = ?" + ";",

						tableName,

						columnName,

						STREAM_KEY);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", updateLogScoresCmd);
		return updateLogScoresCmd;
	}

}
