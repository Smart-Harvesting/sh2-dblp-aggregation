package de.th_koeln.iws.sh2.aggregation.model.util;

import java.time.Year;
import java.time.format.DateTimeParseException;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvConstraintViolationException;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

public class ConvertStringToYear<T> extends AbstractBeanField<T> {

	/**
	 * Silence code style checker by adding a useless constructor.
	 */
	public ConvertStringToYear() {
	}

	@Override
	protected Year convert(String value) throws CsvDataTypeMismatchException, CsvConstraintViolationException {
		if (StringUtils.isEmpty(value)) {
			return null;
		}

		try {
			return Year.parse(value);
		} catch (DateTimeParseException e) {
			CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(value, this.field.getType(),
					ResourceBundle.getBundle("ConvertStringToLocalDate", this.errorLocale).getString("input.not.year"));
			csve.initCause(e);
			throw csve;
		}
	}

}