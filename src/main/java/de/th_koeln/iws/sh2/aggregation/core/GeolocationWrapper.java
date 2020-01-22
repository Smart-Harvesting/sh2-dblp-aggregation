/**
 * 
 */
package de.th_koeln.iws.sh2.aggregation.core;

import java.sql.Connection;

import de.th_koeln.iws.sh2.aggregation.core.helper.GeolocationFinder;

/**
 * @author mandy
 *
 */
public class GeolocationWrapper extends CoreComponent {

	public static void wrapTo(final Connection connection, final GeolocationFinder finder) {
		finder.wrapTo(connection);
	}
}
