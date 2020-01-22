package de.th_koeln.iws.sh2.aggregation.core.xml.data;

import java.time.Month;
import java.time.Year;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author mandy special thanks to mra
 *
 * TODO: clean up those regexes, make them more restictive (number ranges instead of \d)
 *
 */
public class TitleDateParser {

	private static final Logger LOGGER = LogManager.getLogger(TitleDateParser.class);

	/** Month name regex string. */
	private static String monthRegex = "[Jj]anuary|[Ff]ebruary|[Mm]arch|[Aa]pril|[Mm]ay|[Jj]une|[Jj]uly"
			+ "|[Aa]ugust|[Ss]eptember|[Oo]ctober|[Nn]ovember|[Dd]ecember|[Jj]anuar|[Ff]ebruar|[Mm]aerz"
			+ "|[Mm]ärz|[Aa]pril|[Mm]ai|[Jj]uni|[Jj]uli|[Aa]ugust|[Ss]sptemer|[Oo]ktober|[Nn]ovember"
			+ "|[Dd]ezember|[Jj]anvier|[Ff]évrier|[Mm]ars|[Aa]vril|[Mm]ai|[Jj]uin|[Jj]uillet|[Aa]oût"
			+ "|[Ss]eptembre|[Oo]ctobre|[Nn]ovembre|[Dd]écembre";
	/** Month number regex String **/
	private static String monthNumberRegex = "1[0-2]|0?[1-9]";
	/** Year regex string. */
	private static String yearRegex = "(19|20)[0-9]{2}"; //"(19|20|['`])?[0-9]{2}"; //aufpassen beim Parsen!
	/** Day regex string **/
	private static String dayRegex = "[12][0-9]|3[01]|0?[1-9]";

	/**
	 * Simple year pattern: {@code yyyy} or {@code ’yy}
	 */
	private static Pattern simpleYearPattern = Pattern.compile("(.*) (?<year>" + yearRegex + ")(.*)");
	//Pattern.compile("(.*) (?<year>(19|20)[0-9]{2})(.*)");

	/** Month pattern: {@code "dd-dd MONTH yyyy"} */
	private static Pattern dayMonthYearPattern = Pattern
			.compile("(.*)(?<day>[0-9]{1,2}) (?<month>" + monthRegex + "),? (?<year>" + yearRegex + ")(.*)");
	// Pattern.compile("(.*)(?<day>" + dayRegex + ") (?<month>" + monthRegex + "),? (?<year>" + yearRegex + ")(.*)");

	/** Month pattern: {@code "MONTH dd-dd yyyy"} */
	private static Pattern monthDayYearPattern = Pattern.compile(
			"(.*) (?<month>" + monthRegex + ") ([0-9]{1,2} ?[/-] ?)?(?<day>[0-9]{1,2}),? (?<year>" + yearRegex + ")(.*)");
	// Pattern.compile("(.*) (?<month>" + monthRegex + ") (?:" + dayRegex + " ?[/-] ?)?(?<day>" + dayRegex + "),? (?<year>" + yearRegex + ")(.*)");

	/** Month pattern: {@code "MONTH yyyy"} */
	private static Pattern monthYearPattern = Pattern
			.compile("(.*) (?<month>" + monthRegex + "),? (?<year>" + yearRegex + ")(.*)");
	// Pattern.compile("(.*) (?<month>" + monthRegex + "),? (?<year>" + yearRegex + ")(.*)");

	/** Date pattern: {@code "dd.mm.yyyy"} or {@code "dd. mm. yyyy} or {@code "dd.mm.yy} */
	private static Pattern datePattern = Pattern.compile("(.*)(?<day>\\d{1,2})\\. ?(?<month>\\d{1,2})\\. ?(?<year>(19|20)[0-9]{2})(.*)");
	// Pattern.compile("(.*)(?<day>" + dayRegex + "). ?(?<month>" + monthNumberRegex + "). ?(?<year>" + yearRegex + ")(.*)");

	public static Optional<Month> getEventMonth(String title) {
		Optional<Month> month = extractMonth(title);

		return month;
	}

	public static Optional<Year> getEventYear(String title) {
		Optional<Year> year = extractYear(title);

		return year;
	}

	public static Optional<Short> getEventDay(String title) {
		Optional<Short> day = extractDay(title);

		return day;
	}

	private static Optional<Short> extractDay(String title) {
		Matcher dayMonthYearMatcher = dayMonthYearPattern.matcher(title);
		if (dayMonthYearMatcher.matches()) {
			Short day = Short.parseShort(dayMonthYearMatcher.group("day"));
			return Optional.of(day);
		}
		Matcher monthDayYearMatcher = monthDayYearPattern.matcher(title);
		if (monthDayYearMatcher.matches()) {
			Short day = Short.parseShort(monthDayYearMatcher.group("day"));
			return Optional.of(day);
		}
		Matcher dateMatcher = datePattern.matcher(title);
		if(dateMatcher.matches()) {
			Short day = Short.parseShort(dateMatcher.group("day"));
			return Optional.of(day);
		}

		LOGGER.debug("Title did not match any date pattern containing day: " + title);
		return Optional.empty();
	}

	/**
	 * Extract any year information from the given title string.
	 *
	 * @param title
	 *            The title string.
	 */
	private static Optional<Year> extractYear(String title) {
		Matcher dayMonthYearMatcher = dayMonthYearPattern.matcher(title);
		if (dayMonthYearMatcher.matches()) {
			Year year = Year.parse(dayMonthYearMatcher.group("year"));
			return Optional.of(year);
		}
		Matcher monthDayYearMatcher = monthDayYearPattern.matcher(title);
		if (monthDayYearMatcher.matches()) {
			Year year = Year.parse(monthDayYearMatcher.group("year"));
			return Optional.of(year);
		}
		Matcher monthYearMatcher = monthYearPattern.matcher(title);
		if (monthYearMatcher.matches()) {
			Year year = Year.parse(monthYearMatcher.group("year"));
			return Optional.of(year);
		}
		Matcher simpleYearMatcher = simpleYearPattern.matcher(title);
		if (simpleYearMatcher.matches()) {
			Year year = Year.parse(simpleYearMatcher.group("year"));
			return Optional.of(year);
		}
		Matcher dateMatcher = datePattern.matcher(title);
		if(dateMatcher.matches()) {
			Year year = Year.parse(dateMatcher.group("year"));
			return Optional.of(year);
		}

		LOGGER.debug("Title did not match any year pattern: " + title);
		return Optional.empty();
	}

	/**
	 * Extract any month information from the given title string.
	 *
	 * @param title
	 *            The title string.
	 */
	private static Optional<Month> extractMonth(String title) {
		Matcher dayMonthYearMatcher = dayMonthYearPattern.matcher(title);
		if (dayMonthYearMatcher.matches()) {
			return Optional.of(getMonth(dayMonthYearMatcher));
		}
		Matcher monthDayYearMatcher = monthDayYearPattern.matcher(title);
		if (monthDayYearMatcher.matches()) {
			return Optional.of(getMonth(monthDayYearMatcher));
		}
		Matcher monthYearMatcher = monthYearPattern.matcher(title);
		if (monthYearMatcher.matches()) {
			return Optional.of(getMonth(monthYearMatcher));
		}
		Matcher dateMatcher = datePattern.matcher(title);
		if(dateMatcher.matches()) {
			Month month = Month.of(Integer.parseInt(dateMatcher.group("month")));
			return Optional.of(month);
		}

		LOGGER.debug("Title did not match any month pattern: " + title);
		return Optional.empty();
	}

	private static Month getMonth(Matcher dayMonthYearMatcher) {
		String matchedMonth = dayMonthYearMatcher.group("month").toUpperCase();
		switch (matchedMonth) {
		case "JANVIER":
		case "JANUAR":
		case "JANUARY":
			return Month.JANUARY;
		case "FÉVRIER":
		case "FEBRUAR":
		case "FEBRUARY":
			return Month.FEBRUARY;
		case "MÄRZ":
		case "MARS":
		case "MARCH":
			return Month.MARCH;
		case "APRIL":
		case "AVRIL":
			return Month.APRIL;
		case "MAI":
		case "MAY":
			return Month.MAY;
		case "JUIN":
		case "JUNE":
		case "JUNI":
			return Month.JUNE;
		case "JULI":
		case "JULY":
		case "JUILLET":
			return Month.JULY;
		case "AUGUST":
		case "AOÛT":
			return Month.AUGUST;
		case "SEPTEMBER":
		case "SEPTEMBRE":
			return Month.SEPTEMBER;
		case "OCTOBER":
		case "OKTOBER":
		case "OCTOBRE":
			return Month.OCTOBER;
		case "NOVEMBER":
		case "NOVEMBRE":
			return Month.NOVEMBER;
		case "DEZEMBER":
		case "DECEMBER":
		case "DÉCEMBRE":
			return Month.DECEMBER;
		}
		return null;
	}

}