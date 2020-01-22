package de.th_koeln.iws.sh2.aggregation.core;

import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.AFFILIATON_SCORE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.GENERIC_ID;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.INSTITUTION_COUNTRY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.PERSON_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.RAW_AFFILIATON_SCORES;
import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.SIGN_AFFIL_LOCATIONS;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.DETAIL_VIEW;
import static de.th_koeln.iws.sh2.aggregation.core.util.ViewNames.DETAIL_WITH_LOCATIONS_VIEW;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.Tables;
import com.google.common.collect.TreeBasedTable;
import com.google.common.collect.TreeMultimap;

/**
 * Aggregator for affiliation scores of conferences.
 * 
 * <p>
 * Depends on {@code SIGN_AFFIL_LOCATIONS}.
 * 
 * <p>
 * Creates {@code DETAIL_WITH_LOCATIONS_VIEW}, {@code RAW_AFFIL_SCORES}.
 * 
 * <p>
 * TODO:
 *
 * @author michels
 *
 */
public class AffiliationAggregator extends CoreAggregator {

	private static final Logger LOGGER = LogManager.getLogger(AffiliationAggregator.class);
	private static final int BATCH_SIZE = 1000;

	/**
	 * Wrapper method. Reads the required data, performs the calculation and
	 * batch-inserts the results in a new table.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 */
	public static void wrapTo(final Connection connection, final int lteYear) {

		materializeView(connection, DETAIL_WITH_LOCATIONS_VIEW, getMaterializeDetailWithLocationsViewCmd(lteYear));

		final ImmutableSortedMap<String, Double> rawAffiliationScores = computeSimpleAffiliationScores(connection,
				lteYear);

		reCreateTable(connection, RAW_AFFILIATON_SCORES, getCreateAffiliationScoresCmd());

		final String insertLogFormat = "{}: Batch-inserting raw citation scores into table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", RAW_AFFILIATON_SCORES);

		try (final PreparedStatement insertAffiliationScores = connection
				.prepareStatement(getInsertAffiliationScoresCmd())) {

			int streamCount = 0;

			for (java.util.Map.Entry<String, Double> entry : rawAffiliationScores.entrySet()) {

				insertAffiliationScores.setString(1, entry.getKey());
				insertAffiliationScores.setDouble(2, entry.getValue());

				insertAffiliationScores.addBatch();

				if ((++streamCount % BATCH_SIZE) == 0) {
					LOGGER.info("Processed {} streams...", streamCount);
					insertAffiliationScores.executeBatch();
				}
			}
			insertAffiliationScores.executeBatch();

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", RAW_AFFILIATON_SCORES), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", RAW_AFFILIATON_SCORES,
				Duration.between(insertStart, Instant.now()));
	}

	private static ImmutableSortedMap<String, Double> computeSimpleAffiliationScores(Connection connection,
			int lteYear) {

		final ImmutableTable<String, String, Integer> authorsPerCite = getAuthorsPerCite(connection, lteYear);
		final ImmutableTable<String, String, ImmutableSetMultimap<String, String>> authorInstitutions = getAuthorInstitutions(
				connection);

		final ImmutableMultiset<String> countryMultiset = authorInstitutions.values().stream().map(e -> e.values())
				.flatMap(Collection::stream).collect(ImmutableMultiset.toImmutableMultiset());

		LOGGER.debug("[{}] country-set size: {}", lteYear, countryMultiset.elementSet().size());

		final SortedMap<String, Double> affiliationScores = new TreeMap<>();

		for (final String streamKey : authorInstitutions.rowKeySet()) {

			final ImmutableSet<String> citeKeys = authorInstitutions.row(streamKey).keySet();
			final List<Double> citeAffiliationScores = Lists.newArrayList();

			for (final String citeKey : citeKeys) {

				final ImmutableSetMultimap<String, String> authorCountriesMultimap = authorInstitutions.get(streamKey,
						citeKey);
				final int authorInstanceSize = authorCountriesMultimap.keySet().stream()
						.filter(personKey -> authorCountriesMultimap.get(personKey).size() > 0)
						.collect(Collectors.toSet()).size();
				final int authorPopulationSize = authorsPerCite.get(streamKey, citeKey);
				final double citeCoverage = (authorInstanceSize * 1.0) / authorPopulationSize;

				//				LOGGER.debug("[{}, {}, {}] cite author countries: {}", lteYear, streamKey, citeKey, authorCountriesMultimap);
				LOGGER.debug("[{}, {}, {}] cite author instance size: {}", lteYear, streamKey, citeKey,
						authorInstanceSize);
				LOGGER.debug("[{}, {}, {}] cite author population size: {}", lteYear, streamKey, citeKey,
						authorPopulationSize);
				LOGGER.debug("[{}, {}, {}] cite coverage: {}", lteYear, streamKey, citeKey, citeCoverage);

				if (citeCoverage > 0) {
					final ImmutableMultiset<String> citeCountryMultiset = authorInstitutions.get(streamKey, citeKey)
							.values().stream().collect(ImmutableMultiset.toImmutableMultiset());

					final int citeCountrySetCount = citeCountryMultiset.elementSet().size();
					final double citeInternationality = (citeCountrySetCount * 1.0) / countryMultiset.elementSet().size();

					LOGGER.debug("[{}, {}, {}] cite country-set size: {}", lteYear, streamKey, citeKey,
							citeCountrySetCount);
					LOGGER.debug("[{}, {}, {}] cite internationality: {}", lteYear, streamKey, citeKey,
							citeInternationality);

					citeAffiliationScores.add(citeCoverage * citeInternationality);
				}
			}
			LOGGER.debug("[{}, {}] cites' raw affiliation scores: {}", lteYear, streamKey, citeAffiliationScores);

			final double rawAffilScore = citeAffiliationScores.stream()
					.collect(Collectors.summingDouble(Double::doubleValue)) / citeAffiliationScores.size();

			if (Double.isFinite(rawAffilScore)) {
				LOGGER.debug("[{}, {}] raw affiliation score: {}", lteYear, streamKey, rawAffilScore);
				affiliationScores.put(streamKey, rawAffilScore);
			}
		}
		return ImmutableSortedMap.copyOf(affiliationScores);
	}

	private static ImmutableSortedMap<String, Double> computeEntropicAffiliationScores(final Connection connection,
			final int lteYear) {

		final ImmutableTable<String, String, Integer> authorsPerCite = getAuthorsPerCite(connection, lteYear);
		final ImmutableTable<String, String, ImmutableSetMultimap<String, String>> authorInstitutions = getAuthorInstitutions(
				connection);

		final ImmutableMultiset<String> countryMultiset = authorInstitutions.values().stream().map(e -> e.values())
				.flatMap(Collection::stream).collect(ImmutableMultiset.toImmutableMultiset());
		final ImmutableMap<String, Double> countryProbabilities = getCountryProbabilities(countryMultiset);

		final double idealEntropy = Math.log(countryMultiset.elementSet().size()) / Math.log(2);
		//		final double actualEntropy = getActualEntropy(countryProbabilities);
		//		final double normalizedEntropy = actualEntropy / idealEntropy;

		final SortedMap<String, Double> affiliationScores = new TreeMap<>();

		for (final String streamKey : authorInstitutions.rowKeySet()) {

			final ImmutableSet<String> citeKeys = authorInstitutions.row(streamKey).keySet();
			final List<Double> citeScores = Lists.newArrayList();

			for (final String citeKey : citeKeys) {

				final ImmutableSetMultimap<String, String> authorCountriesMultimap = authorInstitutions.get(streamKey,
						citeKey);
				final int authorInstanceSize = authorCountriesMultimap.keySet().stream()
						.filter(personKey -> authorCountriesMultimap.get(personKey).size() > 0)
						.collect(Collectors.toSet()).size();
				final int authorPopulationSize = authorsPerCite.get(streamKey, citeKey);
				final double citeCoverage = (authorInstanceSize * 1.0) / authorPopulationSize;

				if (citeCoverage > 0) {
					final ImmutableMultiset<String> citeCountryMultiset = authorInstitutions.get(streamKey, citeKey)
							.values().stream().collect(ImmutableMultiset.toImmutableMultiset());

					double citeBits = 0;
					for (final Entry<String> entry : citeCountryMultiset.entrySet()) {
						citeBits += entry.getCount() * getInformationContent(entry.getElement(), countryProbabilities);
					}
					final int citeSymbolCount = citeCountryMultiset.size();
					final double citeBitsPerSymbol = citeBits / citeSymbolCount;
					final double normalizedCiteBitsPerSymbol = citeBitsPerSymbol / idealEntropy;

					final int citeCountrySetCount = citeCountryMultiset.elementSet().size();
					final double citeInternationality = (citeCountrySetCount * 1.0) / countryMultiset.elementSet().size();

					citeScores.add(citeCoverage * normalizedCiteBitsPerSymbol * citeInternationality);

					//					System.out.println(String.format(
					//
					//							"(%s, %s): cov = %4.3f, " + "norm_ic_p_sc = %4.3f, cite_intl = %4.3f",
					//
					//							streamKey, citeKey, citeCoverage, normalizedCiteBitsPerSymbol, citeInternationality));
				}
			}
			final double streamScore = citeScores.stream().collect(Collectors.summingDouble(Double::doubleValue))
					/ citeScores.size();
			if (Double.isFinite(streamScore)) {
				affiliationScores.put(streamKey, streamScore);
			}
			//			System.out.println(String.format("(%s): cov = %4.3f, avg_cs = %4.3f, sc = %4.3f", streamKey, streamCoverage,
			//					citeScoreAverage, streamScore));
		}
		//		System.out.println("actual_entropy = " + actualEntropy);
		//		System.out.println("ideal_entropy = " + idealEntropy);
		//		System.out.println("normalized_entropy = " + normalizedEntropy);

		return ImmutableSortedMap.copyOf(affiliationScores);
	}

	private static double getActualEntropy(final ImmutableMap<String, Double> countryProbabilities) {

		double actualEntropy = 0;

		for (String country : countryProbabilities.keySet()) {
			actualEntropy += countryProbabilities.get(country) * getInformationContent(country, countryProbabilities);
		}
		return actualEntropy;
	}

	private static double getInformationContent(final String country,
			final ImmutableMap<String, Double> countryProbabilities) {
		return -1.0 * (Math.log(countryProbabilities.get(country)) / Math.log(2));
	}

	private static ImmutableMap<String, Double> getCountryProbabilities(ImmutableMultiset<String> countryMultiset) {
		final TreeMap<String, Double> countryProbabilities = Maps.newTreeMap();
		for (String country : countryMultiset.elementSet()) {
			countryProbabilities.put(country, (countryMultiset.count(country) * 1.0) / countryMultiset.size());
		}
		return ImmutableMap.copyOf(countryProbabilities);
	}

	/**
	 * Helper method to get the number of distinct authors of an event.
	 *
	 * @param connection the database connection
	 * @param lteYear    the year up to which conference records should be taken
	 *                   into account
	 * 
	 * @return defensive copy of a tree-based table (row keys = {@code STREAM_KEY}s;
	 *         column keys = {@code CITE_KEY}s; cell values = number of distinct
	 *         authors)
	 */
	protected static ImmutableTable<String, String, ImmutableSetMultimap<String, String>> getAuthorInstitutions(
			final Connection connection) {

		final TreeBasedTable<String, String, SetMultimap<String, String>> authorInstitutions = TreeBasedTable.create();
		//		final SetMultimap<String, String> authorInstitutions = TreeMultimap.create();

		final String aggregateLogFormat = "{}: Aggregating institution locations per author from table '{}'";
		final Instant aggregateStart = Instant.now();
		LOGGER.info(aggregateLogFormat, "START", DETAIL_WITH_LOCATIONS_VIEW);

		try (final PreparedStatement aggregateAuthorInstitutions = connection
				.prepareStatement(getAggregateAuthorInstitutionsCmd())) {

			connection.setAutoCommit(false);
			aggregateAuthorInstitutions.setFetchSize(100000);

			final ResultSet authorInstitutionsResults = aggregateAuthorInstitutions.executeQuery();

			while (authorInstitutionsResults.next()) {

				final String streamKey = authorInstitutionsResults.getString(STREAM_KEY);
				final String citeKey = authorInstitutionsResults.getString(CITE_KEY);
				final String personKey = authorInstitutionsResults.getString(PERSON_KEY);
				final String institutionCountry = authorInstitutionsResults.getString(INSTITUTION_COUNTRY);

				SetMultimap<String, String> authorInstitutionMap = authorInstitutions.get(streamKey, citeKey);

				if (authorInstitutions.get(streamKey, citeKey) == null) {
					authorInstitutionMap = TreeMultimap.create();
					authorInstitutions.put(streamKey, citeKey, authorInstitutionMap);
				}

				if (institutionCountry != null) {
					authorInstitutionMap.put(personKey, institutionCountry);
				}

			}
			connection.commit();
			connection.setAutoCommit(true);

		} catch (SQLException e) {
			LOGGER.fatal(new ParameterizedMessage(aggregateLogFormat, "FAILED", DETAIL_WITH_LOCATIONS_VIEW), e);
			System.exit(1);
		}
		LOGGER.info(aggregateLogFormat + " (Duration: {})", "END", DETAIL_WITH_LOCATIONS_VIEW,
				Duration.between(aggregateStart, Instant.now()));
		return ImmutableTable.copyOf(authorInstitutions.cellSet().stream()
				.map(cell -> Tables.immutableCell(cell.getRowKey(), cell.getColumnKey(),
						ImmutableSetMultimap.copyOf(cell.getValue())))
				.collect(ImmutableTable.toImmutableTable(Cell::getRowKey, Cell::getColumnKey, Cell::getValue)));
	}

	private static String getAggregateAuthorInstitutionsCmd() {

		final String aggregateAuthorInstitutionsCmd = String.format(

				"select * from %s;",

				DETAIL_WITH_LOCATIONS_VIEW);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", aggregateAuthorInstitutionsCmd);
		return aggregateAuthorInstitutionsCmd;
	}

	private static String getMaterializeDetailWithLocationsViewCmd(int lteYear) {

		final String innerSelectFilteredDetails = String.format(

				"select details.%s, details.%s, details.%s, details.%s "

						+ "from %s details "

						+ "where %s <= '%d-12-31'",

						STREAM_KEY, CITE_KEY, RECORD_KEY, PERSON_KEY,

						DETAIL_VIEW,

						C_DATE, lteYear);

		final String innerSelectLocations = String.format(

				"select distinct sal.%s, sal.%s, sal.%s "

						+ "from %s sal "

						+ "where sal.%s is not null",

						RECORD_KEY, PERSON_KEY, INSTITUTION_COUNTRY,

						SIGN_AFFIL_LOCATIONS,

						INSTITUTION_COUNTRY);

		final String selectView = String.format(

				"select distinct filtered_details.%s, filtered_details.%s, filtered_details.%s, locations.%s "

						+ "from (%s) filtered_details left join (%s) locations "

						+ "using (%s, %s)",

						STREAM_KEY, CITE_KEY, PERSON_KEY, INSTITUTION_COUNTRY,

						innerSelectFilteredDetails, innerSelectLocations,

						RECORD_KEY, PERSON_KEY);

		final String materializeDetailWithLocationsViewCmd = String.format(

				"create materialized view %s as %s" + ";",

				DETAIL_WITH_LOCATIONS_VIEW, selectView);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL",
				materializeDetailWithLocationsViewCmd);
		return materializeDetailWithLocationsViewCmd;
	}

	private static String getCreateAffiliationScoresCmd() {

		final String createAffiliationScoresCmd = String.format(

				"create table if not exists %s ( "

						+ "%s bigserial primary key, "

						+ "%s varchar(255) not null, " + "%s numeric not null " + ")" + ";",

						RAW_AFFILIATON_SCORES,

						GENERIC_ID,

						STREAM_KEY, AFFILIATON_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", createAffiliationScoresCmd);
		return createAffiliationScoresCmd;
	}

	private static String getInsertAffiliationScoresCmd() {

		final String insertAffiliationScoresCmd = String.format(

				"insert into %s "

						+ "( " + "%s, %s" + " ) "

						+ "values (?, ?)" + ";",

						RAW_AFFILIATON_SCORES,

						STREAM_KEY, AFFILIATON_SCORE);

		LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", insertAffiliationScoresCmd);
		return insertAffiliationScoresCmd;
	}

}
