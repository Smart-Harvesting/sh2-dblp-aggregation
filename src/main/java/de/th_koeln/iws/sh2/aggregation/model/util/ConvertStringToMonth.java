package de.th_koeln.iws.sh2.aggregation.model.util;

import java.time.DateTimeException;
import java.time.Month;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvConstraintViolationException;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

public class ConvertStringToMonth<T> extends AbstractBeanField<T> {

	/**
	 * Silence code style checker by adding a useless constructor.
	 */

	public ConvertStringToMonth() {
	}

	@Override
	protected Month convert(String value) throws CsvDataTypeMismatchException, CsvConstraintViolationException {
		if (StringUtils.isEmpty(value)) {
			return null;
		}

		try {
			return Month.of(Integer.parseInt(value));
		} catch (DateTimeException e) {
			CsvDataTypeMismatchException csve = new CsvDataTypeMismatchException(value, this.field.getType(),
					ResourceBundle.getBundle("ConvertStringToLocalDate", this.errorLocale).getString("input.not.month"));
			csve.initCause(e);
			throw csve;
		}
	}

}