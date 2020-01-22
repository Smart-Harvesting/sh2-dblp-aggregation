package de.th_koeln.iws.sh2.aggregation.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.th_koeln.iws.sh2.aggregation.config.PropertiesUtil;

/**
 * @author michels, mandy
 *
 */
public class DatabaseManager {
	private Connection connection;
	private static DatabaseManager instance;

	private static final Logger LOGGER = LogManager.getLogger(DatabaseManager.class);

	private DatabaseManager(final Connection connection) {
		this.connection = connection;
	}

	/**
	 * Get an instance of the DatabaseManager. Singleton Pattern.
	 *
	 */
	public static DatabaseManager getInstance() {
		// return instance if it is not null and the connection is alive
		try {
			if (instance != null) {
				if ((instance.getConnection() != null) && !instance.getConnection().isClosed()) {
					return instance;
				}
			}
		} catch (SQLException e) {
			LOGGER.error("SQLException occured while trying to get DatabaseManager instance.", e);
		}

		// create new connection and instance for it
		final Connection connection = connect();

		instance = new DatabaseManager(connection);

		return instance;
	}

	/**
	 * Connect to the database
	 *
	 * @return the database connection
	 */
	private static Connection connect() {
		Connection connection = null;

		try {
			Properties databaseProperties = PropertiesUtil.loadDatabaseConfig();

			String hostName = databaseProperties.getProperty("host");
			String dbName = databaseProperties.getProperty("dbname");
			String portNumber = databaseProperties.getProperty("port");

			String url = new StringBuilder().append("jdbc:postgresql://").append(hostName).append(":")
					.append(portNumber).append("/").append(dbName).toString();
			connection = DriverManager.getConnection(url, databaseProperties);
			LOGGER.debug("Connected to the database");
			DatabaseMetaData dbMetaData = connection.getMetaData();
			LOGGER.debug("Driver name: " + dbMetaData.getDriverName());
			LOGGER.debug("Driver version: " + dbMetaData.getDriverVersion());
			LOGGER.debug("Product name: " + dbMetaData.getDatabaseProductName());
			LOGGER.debug("Product version: " + dbMetaData.getDatabaseProductVersion());
			LOGGER.debug("--------------------\n");

		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
		}

		return connection;
	}

	/**
	 * Issue a query to the database
	 *
	 * @param sqlQuery SQL query string
	 * @return the {@link java.sql.ResultSet} for the query
	 */
	public ResultSet query(final String sqlQuery) {
		ResultSet resultSet = null;
		Statement statement = null;
		try {
			statement = this.connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);

		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			this.close(statement);
		}
		return resultSet;
	}

	/**
	 * Close a statement.
	 *
	 * @param statement the statement to close
	 */
	public void close(Statement statement) {
		if (statement == null) {
			return;
		}

		try {
			statement.close();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void close(ResultSet resultSet, boolean closeConnection) {
		if (resultSet == null) {
			return;
		}

		// close statement
		try {
			Statement statement = resultSet.getStatement();

			if (statement != null) {
				statement.close();
			}

		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
		}

		try {
			resultSet.close();
		} catch (SQLException e) {
			LOGGER.error(e.getMessage(), e);
		}

		if (closeConnection) {
			try {
				this.connection.close();
			} catch (SQLException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * @return the current connection
	 */
	public Connection getConnection() {
		return this.connection;
	}
}