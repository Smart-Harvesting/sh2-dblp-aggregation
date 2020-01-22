package de.th_koeln.iws.sh2.aggregation.core.xml;

import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * Parser for hdblp data. Uses a SAX parsing approach.
 *
 * @author mandy
 *
 */
public class HdblpParser {

	private XMLReader saxParser;
	private HdblpHandler handler;

	/**
	 * Constructor.
	 *
	 * @param processor
	 *            the processor responsible for processing the entries in the XML
	 *            file.
	 * @param dtdPath path to the dblp.dtd file
	 */
	public HdblpParser(IDblpEntryProcessor processor, String dtdPath) {
		this.handler = new HdblpHandler(processor);
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setValidating(false);
		try {
			this.saxParser = factory.newSAXParser().getXMLReader();
			this.saxParser.setEntityResolver(new DtdEntityResolver(dtdPath));
			this.saxParser.setContentHandler(this.handler);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Process the xml.
	 *
	 * @param xml
	 *            Path to the xml file.
	 */
	public void process(String xml) {

		if (null == this.saxParser) {
			System.err.println("Something went wrong while creating the SAX Parser. Returning empty result.");
			return;
		}

		try {
			this.saxParser.parse(new InputSource(new FileInputStream(xml)));
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}