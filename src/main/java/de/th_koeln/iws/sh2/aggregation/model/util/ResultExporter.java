package de.th_koeln.iws.sh2.aggregation.model.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import de.th_koeln.iws.sh2.aggregation.model.ConferenceStream;


/**
 * Utility class for exporting results in several formats.
 *
 * @author mandy
 *
 */
public class ResultExporter {

	private static Logger LOGGER = LogManager.getLogger(ResultExporter.class);

	/**
	 * Export result in wiki markup format.
	 *
	 * @param result
	 *            map associating a {@link ConferenceStream} with a score.
	 * @param targetFile
	 *            path to the target file
	 * @throws IOException
	 */
	public static void exportResult(Map<ConferenceStream, Double> result, String targetFile){
		try(FileWriter writer = new FileWriter(targetFile)) {
			writer.write(String.format("h1. Results\n\n"));
			writer.write(String.format("table{ border: 1px solid; width:50%%; }.\n"));
			writer.write(String.format("| *conf* | *score* | \n"));

			result.forEach((conf, score) -> {
				try {
					writer.write(String.format("|  %s  | %.2f | \n", conf, score));
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			LOGGER.info("Wrote wiki markup to: " + targetFile);
		} catch (IOException e) {
			LOGGER.error(String.format("Could not write file '%s' in wiki markup format", targetFile), e);
		}
	}

	/**
	 * Export result in trec format.
	 *
	 * @param ranked
	 *            ranked list of {@link ConferenceStream}s
	 * @param scored
	 *            map associating a {@link ConferenceStream} with a score.
	 * @param targetRunFilePath
	 *            path to the target file
	 * @param runId
	 *            ID of the current run
	 * @throws IOException
	 */
	public static void exportTrecResult(ArrayList<ConferenceStream> ranked, Map<ConferenceStream, Double> scored,
			Path targetRunFilePath, String runId) {
		File file = targetRunFilePath.toFile();
		try (FileWriter writer = new FileWriter(file)) {
			String topicNum = "topic-001";
			int rank = 0;
			for (ConferenceStream conf : ranked) {
				writer.write(String.format("%s\t%d\t%s\t%d\t%.10f\t%s\n", topicNum, 0, conf.getKey(), rank,
						scored.get(conf), runId));
				rank++;
			}
			LOGGER.info("Wrote Trec result to: " + targetRunFilePath);
		} catch (IOException e) {
			LOGGER.error(String.format("Could not write trec result file '%s'", targetRunFilePath), e);
		}
	}

	/**
	 * Export result in wiki markup format.
	 *
	 * @param ranked
	 *            ranked list of {@link ConferenceStream}s
	 * @param scored
	 *            map associating a {@link ConferenceStream} with a score.
	 * @param targetFile
	 *            path to the target file
	 * @throws IOException
	 */
	public static void exportResult(ArrayList<ConferenceStream> ranked, Map<ConferenceStream, Double> scored,
			Path targetFilePath) {
		try (FileWriter writer = new FileWriter(targetFilePath.toFile())) {
			writer.write(String.format("h1. Results\n\n"));
			writer.write(String.format("table{ border: 1px solid; width:50%%; }.\n"));
			writer.write(String.format("| *conf* | *score* | \n"));

			ranked.forEach(conf -> {
				try {
					writer.write(String.format("|  %s  | %.2f | \n", conf, scored.get(conf)));
				} catch (IOException e) {
					e.printStackTrace();
				}
			});
			LOGGER.info("Wrote wiki markup to: " + targetFilePath);
		} catch (IOException e) {
			LOGGER.error(String.format("Could not write ranking file '%s' in trec format", targetFilePath), e);
		}
	}

	/**
	 * Export the gold standard in trec qrels format.
	 *
	 * @param gold
	 *            map associating a {@link ConferenceStream} with a pseudo-relevance
	 *            score.
	 * @param targetQrelFilePath
	 *            path to the target file
	 * @throws IOException
	 */
	public static void exportTrecQrel(Map<ConferenceStream, Integer> gold, Path targetQrelFilePath) {
		File file = targetQrelFilePath.toFile();

		try (FileWriter writer = new FileWriter(file)) {
			String topicNum = "topic-001";
			for (Entry<ConferenceStream, Integer> entry : gold.entrySet()) {
				writer.write(String.format("%s\t%d\t%s\t%d\n", topicNum, 0, entry.getKey().getKey(), entry.getValue()));
			}
			LOGGER.info("Wrote Trec qrel to: " + targetQrelFilePath);
		} catch (IOException e) {
			LOGGER.error(String.format("Could not write qrel file '%s'", targetQrelFilePath), e);
		}
	}

	/**
	 * Export information on the current run.
	 * @param outputFolder
	 *
	 * @param targetFile
	 *            path to the target file
	 * @param useDead
	 *            value of useDead in run
	 * @param useRank
	 *            value of useRank in run
	 * @param useIntl
	 *            value of useIntl in run
	 * @param useCit
	 *            value of useCit in run
	 * @param useProm
	 *            value of useProm in run
	 * @throws IOException
	 */
	public static void exportInfo(Path outputFilePath, EvaluationConfiguration config) {
		try (FileWriter writer = new FileWriter(outputFilePath.toFile())) {
			writer.write(String.format("Information on output of %s", LocalTime.now().toString()));
			writer.write("\n------------------\n");
			writer.write(String.format("Value of useActive: %b\n", config.isUseActivityScore()));
			writer.write(String.format("Value of useRating: %b\n", config.isUseRatingScore()));
			writer.write(String.format("Value of useIntl: %b\n", config.isUseIntlScore()));
			writer.write(String.format("Value of useCit: %b\n", config.isUseCitationScore()));
			writer.write(String.format("Value of useProm: %b\n", config.isUsePromScore()));
			writer.write(String.format("Value of useSize: %b\n", config.isUseSizeScore()));
			writer.write(String.format("Value of useAffil: %b\n", config.isUseAffilScore()));
			writer.write(String.format("Value of useLog: %b\n", config.isUseLogScore()));
			LOGGER.info("Wrote info to: " + outputFilePath);
		} catch (IOException e) {
			LOGGER.error(String.format("Could not write info file '%s'", outputFilePath), e);
		}
	}

	public static void printToCsv(Path outputFilePath, Table<String, String, BigDecimal> trec_results) {
		final Appendable out = new StringBuilder();

		// add header
		List<String> header = new LinkedList<>();
		header.add("runid");
		header.addAll(trec_results.columnMap().keySet().stream().collect(Collectors.toList()));

		// define printer format
		CSVPrinter printer;
		try (FileWriter writer = new FileWriter(outputFilePath.toFile())) {
			printer = CSVFormat.newFormat(';').withQuoteMode(QuoteMode.MINIMAL).withRecordSeparator('\n')
					.withHeader(header.toArray(new String[header.size()])).print(out);
			printer.printRecords(trec_results.rowMap().entrySet().stream()
					.map(entry -> ImmutableList.builder().add(entry.getKey()).addAll(entry.getValue().values()).build())
					.collect(Collectors.toList()));

			writer.write(out.toString());
		} catch (IOException e) {
			LOGGER.error(String.format("Could not write csv file '%s'", outputFilePath), e);
		}
	}

}
