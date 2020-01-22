package de.th_koeln.iws.sh2.aggregation.core.xml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Custom handler for the SAX parser for parsing hdblp data.
 *
 * @author mandy
 *
 */
public class HdblpHandler extends DefaultHandler {

	private static final Logger LOGGER = LogManager.getLogger(HdblpHandler.class);

	private String keyValue;
	private String mDateValue;
	private String refValue;
	private StringBuffer titleValue;
	private String typeValue;
	private String yearValue;
	private String monthValue;

	private final IDblpEntryProcessor processor;

	private int level = 0;
	private int recordCount = 0;

	private boolean inUrl;
	private boolean inTitle;
	private boolean inYear;
	private boolean inMonth;

	/**
	 * Constructor.
	 *
	 * @param processor
	 *            the processor responsible for processing the entries in the XML
	 *            file.
	 */
	public HdblpHandler(IDblpEntryProcessor processor) {
		this.processor = processor;
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		this.level++;

		if (this.level == 2) {
			this.keyValue = attributes.getValue("key");
			this.mDateValue = attributes.getValue("mdate");
			this.typeValue = qName;
			this.titleValue = new StringBuffer();
		}

		if ((this.level == 3) && qName.equals("url")) {
			this.inUrl = true;
		}

		if ((this.level == 3) && qName.equals("title")) {
			this.inTitle = true;
		}
		if ((this.level == 3) && qName.equals("year")) {
			this.inYear = true;
		}
		if ((this.level == 3) && qName.equals("month")) {
			this.inMonth = true;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		this.level--;

		this.inUrl = false;
		this.inTitle = false;
		this.inYear = false;
		this.inMonth = false;

		// end of record
		if (this.level == 1) {

			this.recordCount++;
			if ((this.recordCount % 100000) == 0) {
				LOGGER.info("parsing hdblp XML (" + this.recordCount + " records) ...");
			}

			if (this.keyValue == null) {
				LOGGER.error("record without key, skipping record: " + this.typeValue);
				this.cleanValues();
				return;
			}
			if (this.mDateValue == null) {
				LOGGER.error("mdate missing, skipping record: " + this.keyValue);
				this.cleanValues();
				return;
			}
			if (this.yearValue == null) {
				LOGGER.debug("year missing: " + this.keyValue);
			}
			this.processor.process(this.keyValue, this.mDateValue, this.refValue, this.titleValue.toString(),
					this.typeValue, this.yearValue, this.monthValue);

			this.cleanValues();
		}
	}

	private void cleanValues() {
		this.keyValue = null;
		this.mDateValue = null;
		this.typeValue = null;
		this.yearValue = null;
		this.monthValue = null;
		this.titleValue = null;
		this.refValue = null;
	}

	@Override
	public void endDocument() throws SAXException {
		LOGGER.info("Parsing ends. Parsed " + this.recordCount + " entries.");
		super.endDocument();
	}

	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		if (this.inUrl) {
			this.refValue = new String(ch, start, length);
		}
		if (this.inTitle) {
			this.titleValue = this.titleValue.append(new String(ch, start, length));
		}
		if (this.inYear) {
			this.yearValue = new String(ch, start, length);
		}
		if (this.inMonth) {
			this.monthValue = new String(ch, start, length);
		}
	}

	/**
	 * @return total number of encountered records
	 */
	public int getRecordCount() {
		return this.recordCount;
	}

}