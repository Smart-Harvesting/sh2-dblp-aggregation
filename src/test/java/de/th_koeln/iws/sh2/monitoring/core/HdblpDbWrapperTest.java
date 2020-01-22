package de.th_koeln.iws.sh2.monitoring.core;

import java.sql.Connection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.th_koeln.iws.sh2.aggregation.core.HdblpDbWrapper;
import de.th_koeln.iws.sh2.aggregation.db.DatabaseManager;

class HdblpDbWrapperTest {

	private static Connection connection;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		connection = DatabaseManager.getInstance().getConnection();
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
	void test() {
		//		HdblpDbWrapper.wrapTo(connection); //nicht als Test geeignet weil es in die Produktiv-DB schreibt
	}

}
