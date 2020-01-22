package de.th_koeln.iws.sh2.aggregation.model.util;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvConstraintViolationException;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

public class ConvertStringToLocalDate<T> extends AbstractBeanField<T> {

	/**
	 * Silence code style checker by adding a useless constructor.
	 */

	public ConvertStringToLocalDate() {
	}

	@Override
	protected Object convert(String value) throws CsvDataTypeMismatchException, CsvConstraintViolationException {
		if (StringUtils.isEmpty(value)) {
			return null;
		}

		try {
			return LocalDate.parse(value);
		} catch (DateTimeParseException e) {
			CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(value, this.field.getType(),
					ResourceBundle.getBundle("ConvertStringToLocalDate", this.errorLocale).getString("input.not.date"));
			csve.initCause(e);
			throw csve;
		}
	}

}