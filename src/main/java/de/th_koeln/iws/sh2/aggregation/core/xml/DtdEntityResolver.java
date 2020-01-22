package de.th_koeln.iws.sh2.aggregation.core.xml;

import java.io.FileReader;
import java.io.IOException;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Custom entity resolver for dblp dtd.
 *
 * @author mandy
 *
 */
public class DtdEntityResolver implements EntityResolver {
	private final String dtdResourcePath;
	private String dblpDtd = "dblp.dtd";

	public DtdEntityResolver(String dtdPath) {
		this.dtdResourcePath = dtdPath;
	}

	@Override
	public InputSource resolveEntity(String publicID, String systemID) throws SAXException, IOException {
		if (systemID.contains(this.dblpDtd)) {
			return new InputSource(new FileReader(this.dtdResourcePath));
		} else
			return null;
	}
}