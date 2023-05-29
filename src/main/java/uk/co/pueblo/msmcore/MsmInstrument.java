package uk.co.pueblo.msmcore;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmInstrument.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	static final int UPDATE_OK = 0;
	static final int UPDATE_WARN = 1;
	static final int UPDATE_ERROR = 2;
	static final int UPDATE_SKIP = 3;

	// Class variables
	static Map<String, int[]> summary = new HashMap<>();
	static int finalStatus = UPDATE_OK;

	// Instance variables
	int workingStatus;

	abstract void update(Map<String, String> inRow) throws IOException;

	Map<String, Object> buildMsmRow(Map<String, String> inRow, Properties props) {
		Map<String, Object> msmRow = new HashMap<>();
		String prop;

		// Validate columns
		int pass;
		String missingCols[] = { "", "", "", "" }; // required, required defaults, optional, optional defaults
		String columnSet = "column.";
		int column = 1;
		
		for (pass = 0; pass < missingCols.length; pass += 2) {
			if (pass == 2) {
				columnSet = columnSet + msmRow.get("xType") + ".";
				column = 1;
			}
			while ((prop = props.getProperty(columnSet + column++)) != null) {
				String propArray[] = prop.split(",");
				String value;

				if (inRow.containsKey(propArray[0])) {
					value = inRow.get(propArray[0]);
				} else {
					// Get default value
					missingCols[pass] = missingCols[pass] + propArray[0] + ", ";
					if (propArray.length == 2) {
						value = propArray[1];
						missingCols[pass + 1] = missingCols[pass + 1] + propArray[0] + ", ";
					} else {
						continue;
					}
				}

				// Now build MSM row
				if (propArray[0].equals("dtLastUpdate") && value.matches("\\d+")) {
					// LocalDateTime value from epoch seconds
					msmRow.put(propArray[0], Instant.ofEpochSecond(Long.parseLong(value)).atZone(SYS_ZONE_ID).toLocalDateTime());
				} else if (propArray[0].equals("dtLastUpdate") && value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")) {
					// LocalDateTime value from UTC string
					msmRow.put(propArray[0], Instant.parse(value).atZone(SYS_ZONE_ID).toLocalDateTime());
				} else if (propArray[0].equals("dt") && value.matches("\\d+")) {
					// LocalDateTime value from epoch seconds and truncated to days
					msmRow.put(propArray[0], Instant.ofEpochSecond(Long.parseLong(value)).atZone(SYS_ZONE_ID).toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
				} else if (propArray[0].equals("dt") && value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")) {
					// LocalDateTime value from UTC string and truncated to days
					msmRow.put(propArray[0], Instant.parse(value).atZone(SYS_ZONE_ID).toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
				} else if (propArray[0].equals("dt") && value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}$")) {
					// LocalDateTime value from CSV date-only string
					msmRow.put(propArray[0], LocalDateTime.parse(value + "T00:00:00"));
				} else if (propArray[0].startsWith("d") || value.matches("\\d+\\.\\d+")) {
					// Double values
					msmRow.put(propArray[0], Double.parseDouble(value));
				} else if (propArray[0].startsWith("x")) {
					// msmquote internal values
					msmRow.put(propArray[0], value);
				} else {
					// And finally assume everything else is a Long value
					msmRow.put(propArray[0], Long.parseLong(value));
				}
			}
		}

		// Emit log messages and set working status
		String logMsg[] = { "Required quote data missing", "Required default values applied", "Optional quote data missing", "Optional default values applied" };
		Level logLevel[] = { Level.ERROR, Level.ERROR, Level.WARN, Level.WARN };
		int tmpStatus;
		for (pass = 0; pass < missingCols.length; pass++) {
			if (!missingCols[pass].isEmpty()) {
				LOGGER.log(logLevel[pass], "{} for symbol {}: {}", logMsg[pass], msmRow.get("xSymbol").toString(), missingCols[pass].substring(0, missingCols[pass].length() - 2));
				if ((tmpStatus = UPDATE_ERROR + 2 - logLevel[pass].intLevel() / 100) > workingStatus) {
					workingStatus = tmpStatus;
				}
			}
		}

		return msmRow;
	}

	void incSummary(String quoteType) {
		// Increment summary counters
		summary.putIfAbsent(quoteType, new int[4]); // OK, warning, error, skipped
		int[] count = summary.get(quoteType);
		count[workingStatus]++;
		summary.put(quoteType, count);
		if (workingStatus > finalStatus && workingStatus < UPDATE_SKIP) {
			finalStatus = workingStatus;
		}
		return;
	}

	public static int logSummary() {
		// Output summary to log
		summary.forEach((key, count) -> {
			int processed = count[0] + count[1] + count[2] + count[3];
			LOGGER.info("Summary for quote type {}: processed={} [OK={}, warnings={}, errors={}, skipped={}]", key, processed, count[0], count[1], count[2], count[3]);
		});
		return finalStatus;
	}

	static Properties openProperties(String propsFile) {
		// Open properties
		Properties props = new Properties();
		try {
			InputStream propsIs = MsmInstrument.class.getClassLoader().getResourceAsStream(propsFile);
			props.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
		return props;
	}
}