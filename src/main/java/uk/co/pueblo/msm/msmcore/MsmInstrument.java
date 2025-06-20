package uk.co.pueblo.msm.msmcore;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
	static final int MAX_SYMBOL_SZ = 12; // Money symbol size excluding exchange prefix
	static final int EXIT_OK = 0;
	static final int EXIT_WARN = 1;
	static final int EXIT_ERROR = 2;

	// Instance variables
	Map<String, int[]> summary = new HashMap<>();
	UpdateStatus updateStatus;
	final List<String[]> msmSymbols = new ArrayList<>();
	final List<String> msmSymbolsCheck = new ArrayList<>();

	// Quote update status
	public enum UpdateStatus {
		OK("updated OK=", EXIT_OK), MISSING_OPTIONAL("missing optional data=", EXIT_WARN), MISSING_REQUIRED("missing required data=", EXIT_ERROR), NOT_FOUND("not found=", EXIT_ERROR),	NO_CHANGE("no change=", EXIT_OK), STALE("stale=", EXIT_WARN), NEW_STALE("new stale=", EXIT_WARN);

		public final String msg;
		public final int exitCode;

		UpdateStatus(String msg, int exitCode) {
			this.msg = msg;
			this.exitCode = exitCode;
		}
	}

	abstract void update(Map<String, String> inRow) throws IOException, MsmInstrumentException;

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
				incSummary(inRow.get("xType"), UpdateStatus.MISSING_REQUIRED); // TODO xType might be null
				throw new MsmInstrumentException("Missing required quote data for symbol " + inRow.get("xSymbol") + ": " + prop); // TODO xSymbol might be null
			}
		}

		// Validate optional values
		String quoteType = inRow.get("xType");
		columnSet = columnSet + quoteType + '.';
		index = 1;
		StringJoiner missingCols[] = { new StringJoiner(", "), new StringJoiner(", ") }; // optional, optional defaults
		while ((prop = props.getProperty(columnSet + index++)) != null) {
			String propArray[] = prop.split(",");
			String column = propArray[0];
			if (inRow.containsKey(column)) {
				outRow.put(column, inRow.get(column));
			} else {
				updateStatus = UpdateStatus.MISSING_OPTIONAL;
				missingCols[0].add(column);
				// Add default value to row
				if (propArray.length == 2) {
					outRow.put(column, propArray[1]);
					missingCols[1].add(column + "=" + propArray[1]);
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
			emitLogMsgs(symbol, new String[] { "Missing optional quote data", "Applied optional quote data default values" }, missingCols);
		} else {
			// Reject if symbol is not in symbols list
			incSummary(quoteType, UpdateStatus.NOT_FOUND);
			throw new MsmInstrumentException("Cannot find symbol " + symbol + " in symbols list");
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

	static Properties loadProperties(String propsFile) throws IOException {
		final Properties props = new Properties();
		InputStream propsIs = MsmInstrument.class.getClassLoader().getResourceAsStream(propsFile);
		props.load(propsIs);
		return props;
	}

	public List<String[]> getSymbols() {
		return msmSymbols;
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
		Set<UpdateStatus> updatedSet = EnumSet.of(UpdateStatus.OK, UpdateStatus.MISSING_OPTIONAL, UpdateStatus.NEW_STALE);
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