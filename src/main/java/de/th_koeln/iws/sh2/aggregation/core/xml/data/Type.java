package de.th_koeln.iws.sh2.aggregation.core.xml.data;

import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

/**
 * Enum representing the type of a dblp entry.
 *
 * @author mandy
 *
 */
public enum Type {
	ARTICLE("article"), BOOK("book"), INCOLLECTION("incollection"), PROCEEDINGS("proceedings"), INPROCEEDINGS(
			"inproceedings"), MASTERSTHESIS("mastersthesis"), PHDTHESIS("phdthesis"), WWW("www"), PERSON("person");
	private String value;

	private static final Map<String, Type> LOOKUP = Maps.uniqueIndex(Arrays.asList(Type.values()), Type::getValue);

	/**
	 * return the type for a given value
	 * @param value String value of the type
	 */
	private Type(String value) {
		this.value = value;
	}

	/**
	 * return the string value of the type
	 * @return String value of the type
	 */
	public String getValue() {
		return this.value;
	}

	/**
	 * return the Type corresponding to a given type String
	 * @param string
	 * @return
	 */
	@Nullable
	public static Type fromString(String string) {
		return LOOKUP.get(string);
	}
}