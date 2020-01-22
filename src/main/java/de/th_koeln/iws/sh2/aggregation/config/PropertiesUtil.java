package de.th_koeln.iws.sh2.aggregation.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

public class PropertiesUtil {

	private static Logger LOGGER = LogManager.getLogger(PropertiesUtil.class);

	public static Properties load(Path pathToFile) {
		if (pathToFile == null || pathToFile.toString().isEmpty())
			throw new IllegalArgumentException(
					"Argument 'pathToFile' was '" + pathToFile + "': must not be null or empty");
		Properties properties = new Properties();
		try (InputStream inputResource = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(pathToFile.toString())) {
			properties.load(inputResource);
		} catch (IOException e) {
			LOGGER.fatal(
					new ParameterizedMessage("Unable to load configuration properties from resource '{}'", pathToFile),
					e);
		}
		return properties;
	}

	public static Properties loadExternalDataSetupConfig() {
		return load(Paths.get(ConfigurationConstants.SETUP_CONFIG));
	}

	public static Properties loadDatabaseConfig() {
		return load(Paths.get(ConfigurationConstants.DATABASE_CONFIG));
	}

}