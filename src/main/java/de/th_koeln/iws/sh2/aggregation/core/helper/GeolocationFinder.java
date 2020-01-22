/**
 * 
 */
package de.th_koeln.iws.sh2.aggregation.core.helper;

import java.sql.Connection;

/**
 * @author mandy
 *
 */
public interface GeolocationFinder {

	void wrapTo(final Connection connection);

}
