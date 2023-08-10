package uk.co.pueblo.msmcore;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmInstrument.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	static final int MAX_SYMBOL_SZ = 15; // Money exchange prefix + symbol
	

	// Class variables
	private static Map<String, int[]> summary = new HashMap<>();
	private static UpdateStatus finalStatus = UpdateStatus.OK;

	// Instance variables
	UpdateStatus workingStatus;
	final List<String[]> msmSymbols = new ArrayList<>();
	final List<String> msmSymbolsCheck = new ArrayList<>();

	// Quote update status
	enum UpdateStatus {
		OK(0, "OK=", Level.INFO), WARN(1, "warnings=", Level.WARN), ERROR(2, "errors=", Level.ERROR), SKIP(3, "skipped=", Level.INFO), STALE(4, "stale=", Level.INFO);

		final int index;
		final String label;
		final Level level;;

		static final int size;
		static {
			size = values().length;
		}

		UpdateStatus(int index, String label, Level level) {
			this.index = index;
			this.label = label;
			this.level = level;
		}
	}

	abstract void update(Map<String, String> inRow) throws IOException;

	Map<String, String> validateQuoteRow(Map<String, String> inRow, Properties props) {
		Map<String, String> outRow = new HashMap<>();
		String prop;
		StringJoiner missingCols[] = { new StringJoiner(", "), new StringJoiner(", "), new StringJoiner(", "), new StringJoiner(", ") }; // required, required defaults, optional, optional defaults
		String columnSet = "column.";
		int pass;
		int index = 1;

		for (pass = 0; pass < missingCols.length; pass += 2) {
			if (pass == 2) {
				columnSet = columnSet + inRow.get("xType") + '.';
				index = 1;
			}
			while ((prop = props.getProperty(columnSet + index++)) != null) {
				String propArray[] = prop.split(",");
				String column = propArray[0];
				if (inRow.containsKey(column)) {
					outRow.put(column, inRow.get(column));
				} else {
					missingCols[pass].add(column);
					// Add default value to row
					if (propArray.length == 2) {
						outRow.put(column, propArray[1]);
						missingCols[pass + 1].add(column);
					}
				}
			}
		}

		String symbol = outRow.get("xSymbol");
		if (msmSymbolsCheck.contains(symbol)) {
			emitLogMsgs(outRow.get("xSymbol"), new String[] { "Required quote data missing", "Required default values applied", "Optional quote data missing", "Optional default values applied" }, missingCols, new UpdateStatus[] { UpdateStatus.ERROR, UpdateStatus.ERROR, UpdateStatus.WARN, UpdateStatus.WARN });
		} else {
			// Reject if symbol is not in symbols list
			LOGGER.error("Cannot find symbol {} in symbols list", symbol);
			workingStatus = UpdateStatus.ERROR;
		}

		return outRow;
	}

	Map<String, Object> buildMsmRow(Map<String, String> inRow, Properties props) {
		Map<String, Object> msmRow = new HashMap<>();
		String prop;
		StringJoiner invalidCols[] = { new StringJoiner(", "), new StringJoiner(", "), new StringJoiner(", "), new StringJoiner(", ") }; // required, required defaults, optional, optional defaults
		String columnSet = "column.";
		int index = 1;

		for (int pass = 0; pass < invalidCols.length; pass += 2) {
			if (pass == 2) {
				columnSet = columnSet + msmRow.get("xType") + '.';
				index = 1;
			}
			while ((prop = props.getProperty(columnSet + index++)) != null) {
				String propArray[] = prop.split(",");
				String propCol = propArray[0];
				String value;

				if (inRow.containsKey(propCol)) {
					value = inRow.get(propCol);
					// Add key and value to MSM row
					while (true) {
						if (propCol.equals("dtLastUpdate") && value.matches("\\d+")) {
							// LocalDateTime value from epoch seconds
							msmRow.put(propCol, Instant.ofEpochSecond(Long.parseLong(value)).atZone(SYS_ZONE_ID).toLocalDateTime());
						} else if (propCol.equals("dtLastUpdate") && value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")) {
							// LocalDateTime value from UTC string
							msmRow.put(propCol, Instant.parse(value).atZone(SYS_ZONE_ID).toLocalDateTime());
						} else if (propCol.equals("dt") && value.matches("\\d+")) {
							// LocalDateTime value from epoch seconds and truncated to days
							msmRow.put(propCol, Instant.ofEpochSecond(Long.parseLong(value)).atZone(SYS_ZONE_ID).toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
						} else if (propCol.equals("dt") && value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")) {
							// LocalDateTime value from UTC string and truncated to days
							msmRow.put(propCol, Instant.parse(value).atZone(SYS_ZONE_ID).toLocalDateTime().truncatedTo(ChronoUnit.DAYS));
						} else if (propCol.equals("dt") && value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}$")) {
							// LocalDateTime value from CSV date-only string
							msmRow.put(propCol, LocalDateTime.parse(value + "T00:00:00"));
						} else if ((propCol.startsWith("d") || propCol.equals("rate")) && value.matches("-?\\d+\\.?\\d+")) {
							// Double values
							msmRow.put(propCol, Double.parseDouble(value));
						} else if (propCol.equals("xSymbol") && value.length() > MAX_SYMBOL_SZ) {
							// symbol to truncate
							String newValue = value.substring(0, MAX_SYMBOL_SZ);
							LOGGER.info("Truncated symbol {} to {}", value, newValue);
							msmRow.put(propCol, newValue);
						} else if (propCol.startsWith("x")) {
							// msmquote internal values
							msmRow.put(propCol, value);
						} else if (value.matches("-?\\d+")) {
							// Long values
							msmRow.put(propCol, Long.parseLong(value));
						} else {
							// Try again with the default value if there is one
							invalidCols[pass].add(propCol + "=" + value);
							if (propArray.length == 2) {
								value = propArray[1];
								invalidCols[pass + 1].add(propCol);
								continue;
							}
						}
						break;
					}
				}
			}
		}

		emitLogMsgs(msmRow.get("xSymbol").toString(), new String[] { "Invalid required quote data", "Required default values applied", "Invalid optional quote data", "Optional default values applied" }, invalidCols, new UpdateStatus[] { UpdateStatus.ERROR, UpdateStatus.ERROR, UpdateStatus.WARN, UpdateStatus.WARN });
		return msmRow;
	}

	private void emitLogMsgs(String symbol, String msgPrefix[], StringJoiner msgCols[], UpdateStatus msgStatus[]) {
		for (int i = 0; i < msgPrefix.length; i++) {
			String columns = msgCols[i].toString();
			if (!columns.isEmpty()) {
				LOGGER.log(msgStatus[i].level, "{} for symbol {}: {}", msgPrefix[i], symbol, columns);
				if (msgStatus[i].level.isMoreSpecificThan(workingStatus.level)) {
					workingStatus = msgStatus[i];
				}
			}
		}
	}

	void incSummary(String quoteType) {
		// Increment summary counters
		summary.putIfAbsent(quoteType, new int[UpdateStatus.size]); // OK, warning, error, skipped, stale
		int[] count = summary.get(quoteType);
		count[workingStatus.index]++;
		summary.put(quoteType, count);
		if (workingStatus.level.isMoreSpecificThan(finalStatus.level)) {
			finalStatus = workingStatus;
		}
		return;
	}

	public static int logSummary() {
		// Output summary to log
		summary.forEach((key, count) -> {
			StringJoiner logSj = new StringJoiner(", ");
			int sum = 0;
			for (UpdateStatus updateStatus : UpdateStatus.values()) {
				logSj.add(updateStatus.label + count[updateStatus.index]);
				sum += count[updateStatus.index];
			}
			LOGGER.info("Summary for quote type {}: processed={} [{}]", key, sum, logSj.toString());
		});

		return 4 - finalStatus.level.intLevel() / 100;
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

	public List<String[]> getSymbols() {
		return msmSymbols;
	}
}