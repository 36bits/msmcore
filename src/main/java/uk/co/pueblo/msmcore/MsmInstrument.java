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
	static final int MAX_SYMBOL_SZ = 12; // Money symbol size excluding exchange prefix

	// Instance variables
	UpdateStatus updateStatus = UpdateStatus.OK;
	final List<String[]> msmSymbols = new ArrayList<>();
	final List<String> msmSymbolsCheck = new ArrayList<>();

	// Quote update status
	public enum UpdateStatus {
		OK(0, "OK=", Level.INFO), WARN(1, "warnings=", Level.WARN), ERROR(2, "errors=", Level.ERROR), FATAL(3, null, Level.FATAL), SKIP(4, "skipped=", Level.INFO), STALE(5, "stale=", Level.INFO);

		public final int index;
		public final String label;
		public final Level level;;

		public static final int size;
		static {
			size = values().length;
		}

		UpdateStatus(int index, String label, Level level) {
			this.index = index;
			this.label = label;
			this.level = level;
		}
	}
	
	abstract UpdateStatus update(Map<String, String> inRow) throws IOException, MsmInstrumentException;

	Map<String, String> validateQuoteRow(Map<String, String> inRow, Properties props) throws MsmInstrumentException {
		Map<String, String> outRow = new HashMap<>();
		String prop;

		// Validate required values
		String columnSet = "column.";
		int index = 1;
		while ((prop = props.getProperty(columnSet + index++)) != null) {
			if (inRow.containsKey(prop)) {
				outRow.put(prop, inRow.get(prop));
			} else {
				throw new MsmInstrumentException("Required quote data missing for symbol " + inRow.get("xSymbol") + ": " + prop, UpdateStatus.ERROR);
			}
		}

		// Validate optional values
		columnSet = columnSet + inRow.get("xType") + '.';
		index = 1;
		StringJoiner missingCols[] = { new StringJoiner(", "), new StringJoiner(", ") }; // optional, optional defaults
		while ((prop = props.getProperty(columnSet + index++)) != null) {
			String propArray[] = prop.split(",");
			String column = propArray[0];
			if (inRow.containsKey(column)) {
				outRow.put(column, inRow.get(column));
			} else {
				missingCols[0].add(column);
				updateStatus = UpdateStatus.WARN;
				// Add default value to row
				if (propArray.length == 2) {
					outRow.put(column, propArray[1]);
					missingCols[1].add(column);
				}
			}
		}

		// If symbol does not have an MSM exchange prefix then truncate if required
		String symbol = outRow.get("xSymbol");
		if (symbol.length() > MAX_SYMBOL_SZ && !symbol.matches("^\\$?..:.+")) {
			String newSymbol = symbol.substring(0, MAX_SYMBOL_SZ);
			LOGGER.info("Truncated symbol {} to {}", symbol, newSymbol);
			outRow.put("xSymbol", newSymbol);
			symbol = newSymbol;
		}

		if (msmSymbolsCheck.contains(symbol)) {
			emitLogMsgs(symbol, new String[] { "Optional quote data missing", "Optional default values applied" }, missingCols);
		} else {
			// Reject if symbol is not in symbols list
			throw new MsmInstrumentException("Cannot find symbol " + symbol + " in symbols list", UpdateStatus.ERROR);
		}
		
		return outRow;
	}

	Map<String, Object> buildMsmRow(Map<String, String> inRow, Properties props) {
		Map<String, Object> msmRow = new HashMap<>();
		String prop;
		StringJoiner invalidCols[] = { new StringJoiner(", "), new StringJoiner(", ") }; // invalid quota data, default value
		String columnSet = "column.";
		int index = 1;

		for (int pass = 0; pass < 2; pass++) {
			if (pass == 1) {
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
						} else if (propCol.startsWith("x")) {
							// msmquote internal values
							msmRow.put(propCol, value);
						} else if (value.matches("-?\\d+")) {
							// Long values
							msmRow.put(propCol, Long.parseLong(value));
						} else {
							// Try again with the default value if there is one
							invalidCols[0].add(propCol + "=" + value);
							updateStatus = UpdateStatus.WARN;
							if (propArray.length == 2) {
								value = propArray[1];
								invalidCols[1].add(propCol);
								continue;
							}
						}
						break;
					}
				}
			}
		}	
		emitLogMsgs(msmRow.get("xSymbol").toString(), new String[] { "Invalid quote data", "Default values applied" }, invalidCols);
		return msmRow;
	}

	private void emitLogMsgs(String symbol, String msgPrefix[], StringJoiner msgCols[]) {
		for (int i = 0; i < msgPrefix.length; i++) {
			String columns = msgCols[i].toString();
			if (!columns.isEmpty()) {
				LOGGER.warn("{} for symbol {}: {}", msgPrefix[i], symbol, columns);
			}
		}
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