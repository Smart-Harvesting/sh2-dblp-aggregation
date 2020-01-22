/**
 * 
 */
package de.th_koeln.iws.sh2.aggregation.core.helper;

import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.HISTORICAL_RECORDS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import de.th_koeln.iws.sh2.aggregation.config.PropertiesUtil;
import de.th_koeln.iws.sh2.aggregation.core.util.ColumnNames;
import de.th_koeln.iws.sh2.aggregation.core.util.TableNames;
import de.th_koeln.iws.sh2.aggregation.core.util.ViewNames;

/**
 * Geolocation finder using an external python script, which relies on python
 * library geotext.
 * 
 * @author mandy
 *
 */
public class PythonGeolocationFinder implements GeolocationFinder {

	private static final String SCRIPT_GEOLOCATIONFINDER = "script/geolocationfinder.pyt";
	private static Logger LOGGER = LogManager.getLogger(PythonGeolocationFinder.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.th_koeln.iws.sh2.monitoring.core.helper.GeolocationFinder#wrapTo(java.sql.
	 * Connection)
	 * 
	 */
	@Override
	public void wrapTo(final Connection connection) {
		// prepare the python script to run using a template script and a temporary file
		// created with values for template variables
		final String scriptPath = this.prepareScript(PropertiesUtil.loadDatabaseConfig());
		if (null == scriptPath)
			return;

		LOGGER.debug("Found geolocationfinder script in path: " + scriptPath);

		final List<String> command = new ArrayList<>();
		command.add("python");
		command.add(scriptPath);
		final ProcessBuilder processBuilder = new ProcessBuilder(command);

		final String insertLogFormat = "{}: Updating table '{}'";
		final Instant insertStart = Instant.now();
		LOGGER.info(insertLogFormat, "START", HISTORICAL_RECORDS);

		try {

			Process process = processBuilder.start();

			try (BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
					BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {

				// read the standard output from the command
				for (String line = stdInput.readLine(); line != null; line = stdInput.readLine()) {
					LOGGER.info(line);
				}

				// read any errors from the attempted command
				for (String line = stdError.readLine(); line != null; line = stdError.readLine()) {
					LOGGER.warn(line);
				}
			}
		} catch (IOException e) {
			LOGGER.fatal(new ParameterizedMessage(insertLogFormat, "FAILED", HISTORICAL_RECORDS), e);
			System.exit(1);
		}
		LOGGER.info(insertLogFormat + " (Duration: {})", "END", HISTORICAL_RECORDS,
				Duration.between(insertStart, Instant.now()));
	}

	/**
	 * Prepare the temporary python script: Read the template script and replace
	 * template variables with constant values from config and table/view/column
	 * names.
	 * 
	 * @param databaseProperties
	 *            properties for database connection
	 * @return path to the prepared script
	 */
	private String prepareScript(Properties databaseProperties) {
		String originalScriptPath = PythonGeolocationFinder.class.getClassLoader()
				.getResource(SCRIPT_GEOLOCATIONFINDER).getPath();

		Map<String, String> valuesMap = new HashMap<>();

		String hostName = databaseProperties.getProperty("host");
		String dbName = databaseProperties.getProperty("dbname");
		String portNumber = databaseProperties.getProperty("port");

		valuesMap.put("dbName", dbName);
		valuesMap.put("hostName", hostName);
		valuesMap.put("port", portNumber);

		valuesMap.put("userName", databaseProperties.getProperty("user"));
		valuesMap.put("password", databaseProperties.getProperty("password"));
		valuesMap.put("dblp_historical_records_table", TableNames.HISTORICAL_RECORDS);
		valuesMap.put("dblp_conference_proceedings_view", ViewNames.PROCEEDINGS_VIEW);
		valuesMap.put("stream_key_col", ColumnNames.STREAM_KEY);
		valuesMap.put("record_key_col", ColumnNames.RECORD_KEY);
		valuesMap.put("record_title_col", ColumnNames.RECORD_TITLE);
		valuesMap.put("cite_key_col", ColumnNames.CITE_KEY);
		valuesMap.put("event_country_col", ColumnNames.EVENT_COUNTRY);

		// read original script containing template variables into string
		String templateString = this.read(originalScriptPath);
		StringSubstitutor sub = new StringSubstitutor(valuesMap);
		// replace variables with values
		String resolvedString = sub.replace(templateString);

		// write to temp file
		// return path to temp file
		return this.createTempScript(resolvedString);
	}

	/**
	 * Create a temporary file with the given content.
	 * 
	 * @param scriptContent
	 *            content to write to temp file
	 * @return path to the created temp file
	 */
	private String createTempScript(String scriptContent) {
		File tempFile = null;
		try {
			tempFile = File.createTempFile("geolocationfinder", ".py");
			tempFile.deleteOnExit();
		} catch (IOException e) {
			LOGGER.error("Could not create temporary file.", e);
		}

		try (BufferedWriter br = new BufferedWriter(new FileWriter(tempFile))) {
			br.write(scriptContent);
		} catch (IOException e) {
			LOGGER.error("Could not write to temporary file.", e);
		}
		return null == tempFile ? null : tempFile.getAbsolutePath();
	}

	/**
	 * Read the content of a file at the given location into a String.
	 * 
	 * @param filePath
	 *            path to file
	 * @return file content as String
	 */
	private String read(String filePath) {
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			StringBuffer content = new StringBuffer();
			String line;
			while ((line = br.readLine()) != null) {
				content.append(line).append("\n");
			}
			return content.toString();
		} catch (FileNotFoundException e) {
			LOGGER.error("File could not be found.", e);
		} catch (IOException e) {
			LOGGER.error("Error reading file.", e);
		}
		return null;
	}
}
