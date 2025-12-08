package uk.co.pueblo.msm.msmcore;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmInstrument.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	static final int MAX_SYMBOL_LEN = 12; // length of MSM symbol excluding country prefix
	static final int DATE_LEN = 10; // length of YYYY-MM-DD
	static final int EXIT_OK = 0;
	static final int EXIT_WARN = 1;
	static final int EXIT_ERROR = 2;

	// Instance variables
	MsmDb msmDb;
	Map<String, int[]> summary = new HashMap<>();
	UpdateStatus updateStatus;

	// Quote update status
	public enum UpdateStatus {
		OK("updated OK=", EXIT_OK), MISSING_OPTIONAL("missing optional data=", EXIT_WARN), MISSING_REQUIRED("missing required data=", EXIT_ERROR), INVALID_OPTIONAL("invalid optional data=", EXIT_WARN),
		INVALID_REQUIRED("invalid required data=", EXIT_ERROR), NOT_FOUND("not found=", EXIT_ERROR), NO_CHANGE("no change=", EXIT_OK), STALE("stale=", EXIT_WARN), NEW_STALE("new stale=", EXIT_WARN);

		public final String msg;
		public final int exitCode;

		UpdateStatus(String msg, int exitCode) {
			this.msg = msg;
			this.exitCode = exitCode;
		}
	}

	abstract void update(Map<String, Object> inRow) throws IOException, MsmInstrumentException;
	
	abstract List<String[]> getSymbols() throws IOException;

	Map<String, Object> buildMsmRow(Map<String, Object> inRow, Properties props) throws MsmInstrumentException {

		LOGGER.debug("Build MSM row input: {}", inRow);

		Map<String, Object> msmRow = new HashMap<>();
		String prop;
		Object msmValue;
		String columnSet = "column.";
		int index = 1;

		// Add required values to row
		while ((prop = props.getProperty(columnSet + index++)) != null) {
			if (!inRow.containsKey(prop)) {
				// TODO xType or xSymbol might be null
				incSummary(inRow.get("xType").toString(), UpdateStatus.MISSING_REQUIRED);
				throw new MsmInstrumentException("Missing required quote data for symbol " + inRow.get("xSymbol") + ": " + prop);
			}
			Object inValue = inRow.get(prop);
			if ((msmValue = createMsmColumnValue(prop, inValue)) == null) {
				// TODO xType or xSymbol might be null
				incSummary(inRow.get("xType").toString(), UpdateStatus.INVALID_REQUIRED);
				throw new MsmInstrumentException("Invalid required quote data for symbol " + inRow.get("xSymbol") + ": " + prop + "=" + inValue);
			} else {
				msmRow.put(prop, msmValue);
			}
		}

		// Add optional values to row
		String quoteType = inRow.get("xType").toString();
		columnSet = columnSet + quoteType + '.';
		index = 1;
		StringJoiner badColumns[] = { new StringJoiner(", "), new StringJoiner(", "), new StringJoiner(", ") }; // missing, invalid, defaults
		while ((prop = props.getProperty(columnSet + index++)) != null) {
			String propArray[] = prop.split(",");
			if (!inRow.containsKey(propArray[0])) {
				updateStatus = UpdateStatus.MISSING_OPTIONAL;
				badColumns[0].add(propArray[0]);
			} else if ((msmValue = createMsmColumnValue(propArray[0], inRow.get(propArray[0]))) == null) {
				updateStatus = UpdateStatus.INVALID_OPTIONAL;
				badColumns[1].add(propArray[0] + "=" + inRow.get(propArray[0]));
			} else {
				msmRow.put(propArray[0], msmValue);
				continue;
			}
			// Add default value to row
			if (propArray.length == 2) {
				msmRow.put(propArray[0], propArray[1]);
				badColumns[2].add(propArray[0] + "=" + propArray[1]);
			}
		}

		// Emit log messages
		String[] msgPrefix = { "Missing optional quote data", "Invalid optional quote data", "Applied optional quote data default values" };
		for (int i = 0; i < msgPrefix.length; i++) {
			String columns = badColumns[i].toString();
			if (!columns.isEmpty()) {
				LOGGER.warn("{} for symbol {}: {}", msgPrefix[i], inRow.get("xSymbol"), columns);
			}
		}
		
		LOGGER.debug("Build MSM row output: {}",msmRow);
		return msmRow;
	}

	static private Object createMsmColumnValue(String column, Object object) {
		try {
			if (object instanceof String) {
				// String objects requiring processing
				if (column.equals("dtLastUpdate")) {
					// LocalDateTime value from UTC string
					return Instant.parse(object.toString()).atZone(SYS_ZONE_ID).toLocalDateTime();
				} else if (column.equals("dt") && object.toString().length() == DATE_LEN && object.toString().matches("\\d{4}\\-\\d{2}\\-\\d{2}")) {
					// LocalDateTime value from CSV date-only string
					return LocalDateTime.parse(object.toString() + "T00:00:00");
				} else if (column.equals("dt")) {
					// LocalDateTime value from UTC string and truncated to days
					return Instant.parse(object.toString()).atZone(SYS_ZONE_ID).toLocalDateTime().truncatedTo(ChronoUnit.DAYS);
				} else if (column.equals("xSymbol") && object.toString().length() > MAX_SYMBOL_LEN && !object.toString().matches("\\$?..:.+")) {
					// If symbol does not have an MSM country prefix then truncate if required
					String newSymbol = object.toString().substring(0, MAX_SYMBOL_LEN);
					LOGGER.info("Truncated symbol {} to {}", object, newSymbol);
					return newSymbol;
				} else if (column.charAt(0) == 'x') {
					// msmquote internal values
					return object.toString();
				}
			} else {
				// Non-string objects requiring processing
				if (column.equals("dtLastUpdate")) {
					// LocalDateTime value from epoch seconds
					return Instant.ofEpochSecond(((Double) object).longValue()).atZone(SYS_ZONE_ID).toLocalDateTime();
				} else if (column.equals("dt")) {
					// LocalDateTime value from epoch seconds and truncated to days
					return Instant.ofEpochSecond(((Double) object).longValue()).atZone(SYS_ZONE_ID).toLocalDateTime().truncatedTo(ChronoUnit.DAYS);
				}
			}
			// Everything else
			return (Double) object;
		} catch (Exception e) {
			// Do nothing
		}
		return null;
	}

	static Properties loadProperties(String propsFile) throws IOException {
		final Properties props = new Properties();
		InputStream propsIs = MsmInstrument.class.getClassLoader().getResourceAsStream(propsFile);
		props.load(propsIs);
		return props;
	}

	void incSummary(String quoteType, UpdateStatus updateStatus) {
		summary.putIfAbsent(quoteType, new int[UpdateStatus.values().length]);
		int[] count = summary.get(quoteType);
		count[updateStatus.ordinal()]++;
		summary.put(quoteType, count);
		return;
	}

	public UpdateStatus printSummary() {
		UpdateStatus finalStatus = UpdateStatus.OK;
		int maxExitCode = UpdateStatus.OK.exitCode;
		Set<UpdateStatus> updatedSet = EnumSet.of(UpdateStatus.OK, UpdateStatus.MISSING_OPTIONAL, UpdateStatus.INVALID_OPTIONAL, UpdateStatus.NEW_STALE);
		for (Map.Entry<String, int[]> entry : summary.entrySet()) {
			StringJoiner msgSj = new StringJoiner(", ");
			int total = 0;
			int updated = 0;
			int n = 0;
			for (UpdateStatus updateStatus : UpdateStatus.values()) {
				if ((n = entry.getValue()[updateStatus.ordinal()]) > 0) {
					msgSj.add(updateStatus.msg + n);
					total += n;
					if (updatedSet.contains(updateStatus)) {
						updated += n;
					}
					if (updateStatus.exitCode > maxExitCode) {
						maxExitCode = updateStatus.exitCode;
						finalStatus = updateStatus;
					}
				}
			}
			LOGGER.info("Summary for quote type {}: updated={}/{} [{}]", entry.getKey(), updated, total, msgSj.toString());
		}
		return finalStatus;
	}
}