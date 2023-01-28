package uk.co.pueblo.msmcore;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class MsmSecurity extends MsmInstrument {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmSecurity.class);
	private static final String PROPS_FILE = "MsmSecurity.properties";
	private static final String SEC_TABLE = "SEC";
	private static final String SP_TABLE = "SP";
	private static final int SRC_MANUAL = 5;
	private static final int SRC_ONLINE = 6;

	// Instance variables
	private final Table secTable;
	private final Table spTable;
	private ArrayList<Map<String, Object>> newSpRows = new ArrayList<>();
	private int hsp = 0;

	// Constructor
	public MsmSecurity(Database msmDb) throws IOException {
		super(PROPS_FILE);

		// Open the securities tables
		secTable = msmDb.getTable(SEC_TABLE);
		spTable = msmDb.getTable(SP_TABLE);

		// Get current hsp (SP table index)
		IndexCursor spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());
		spCursor.afterLast();
		if (spCursor.getPreviousRow() != null) {
			hsp = (int) spCursor.getCurrentRowValue(spTable.getColumn("hsp"));
		}
		LOGGER.debug("Current highest hsp={}", hsp);
	}

	/**
	 * Update the SEC and SP tables with the supplied quote row.
	 * 
	 * @param sourceRow the row containing the quote data to update
	 * @return 0 OK; 1 update skipped; 2 warning; 3 error
	 * @throws IOException
	 */
	public int update(Map<String, String> sourceRow) throws IOException {

		// Validate incoming row and process status
		Map<String, Object> msmRow = new HashMap<>(buildMsmRow(sourceRow));
		int updateStatus = Integer.parseInt(msmRow.get("xStatus").toString());
		String quoteType = msmRow.get("xType").toString();
		if (updateStatus == UPDATE_ERROR) {
			return updateStatus;
		}

		String symbol = msmRow.get("xSymbol").toString();
		LOGGER.info("Updating quote data for symbol {}, quote type={}", symbol, quoteType);

		// Truncate symbol if required
		String origSymbol = symbol;
		if (origSymbol.length() > 12) {
			symbol = origSymbol.substring(0, 12);
			LOGGER.info("Truncated symbol {} to {}", origSymbol, symbol);
			msmRow.put("xSymbol", symbol);
		}

		// Find symbol in SEC table
		int hsec = -1;
		Map<String, Object> secRow = null;
		IndexCursor secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		if (secCursor.findFirstRow(Collections.singletonMap("szSymbol", symbol))) {
			secRow = secCursor.getCurrentRow();
			hsec = (int) secRow.get("hsec");
			LOGGER.info("Found symbol {} in SEC table: sct={}, hsec={}", symbol, secRow.get("sct"), hsec);
		} else {
			LOGGER.warn("Cannot find symbol {} in SEC table", symbol);
			return UPDATE_ERROR;
		}

		// Update SEC table
		LocalDateTime quoteTime;
		if (msmRow.containsKey("dtLastUpdate")) {
			quoteTime = (LocalDateTime) msmRow.get("dtLastUpdate");
			// Skip update if quote timestamp is equal to SEC row timestamp
			if (quoteTime.equals((LocalDateTime) secRow.get("dtLastUpdate"))) {
				LOGGER.info("Skipped update for symbol {}, new quote has same timestamp as previous quote: timestamp={}", symbol, quoteTime);
				return UPDATE_SKIP;
			}
			// Merge quote row into SEC row and write to SEC table
			secRow.putAll(msmRow); // TODO Should secRow be sanitised first?
			secCursor.updateCurrentRowFromMap(secRow);
			LOGGER.info("Updated SEC table for symbol {}", symbol);
		} else {
			quoteTime = (LocalDateTime) msmRow.get("dt");
		}

		// Add SP table values to quote row
		msmRow.put("dtSerial", LocalDateTime.now()); // TODO Confirm assumption that dtSerial is timestamp of record update
		msmRow.put("src", (long) SRC_ONLINE);

		// Update SP table with quote row
		IndexCursor spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());
		Map<String, Object> spRowPattern = new HashMap<>();
		spRowPattern.put("hsec", hsec);
		Iterator<Row> spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).forward().iterator();
		Map<String, Object> spRow = new HashMap<>();
		Map<String, Object> highestSpRow = new HashMap<>();
		if (spIt.hasNext()) {
			// Get dates from first and last rows for this hsec
			spRow = spIt.next();
			LocalDate firstDate = ((LocalDateTime) spRow.get("dt")).toLocalDate();
			spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).reverse().iterator();
			spRow = spIt.next();
			LocalDate lastDate = ((LocalDateTime) spRow.get("dt")).toLocalDate();

			// Get difference in days between dates of first and last rows and quote date
			LocalDate quoteDate = ((LocalDateTime) msmRow.get("dt")).toLocalDate();
			// long firstDaysDiff = Math.abs(Duration.between(firstDate, quoteDate).toDays());
			long firstDaysDiff = Math.abs(ChronoUnit.DAYS.between(firstDate, quoteDate));
			long lastDaysDiff = Math.abs(ChronoUnit.DAYS.between(lastDate, quoteDate));
			LOGGER.debug("Timestamps: first={}, last={}, quote={}", firstDate, lastDate, quoteDate);
			LOGGER.debug("Days difference: first->quote={}, last->quote={}", firstDaysDiff, lastDaysDiff);

			// Create forward iterator again if first row date is closest to quote date
			if (firstDaysDiff < lastDaysDiff) {
				spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).reverse().iterator();
				spRow = spIt.next();
			}

			// Search SP table for same-day quote or most recent previous quote
			LocalDate rowDate;
			LocalDate highestDate = LocalDate.ofEpochDay(0);
			while (true) {
				LOGGER.debug("Highest seen date: {}", highestDate);
				LOGGER.debug(spRow);
				int src = (int) spRow.get("src");
				rowDate = ((LocalDateTime) spRow.get("dt")).toLocalDate();
				// Update highest seen date and row
				if (rowDate.isAfter(highestDate)) {
					if (rowDate.isBefore(quoteDate)) { // update for any price source
						highestDate = rowDate;
						highestSpRow = spRow;
					} else if (rowDate.equals(quoteDate) && src < SRC_MANUAL) { // update for transaction price source
						highestDate = rowDate;
						highestSpRow = spRow;
					}
				}
				// Check for existing quote for this quote date
				if (rowDate.equals(quoteDate)) {
					if (src == SRC_ONLINE || src == SRC_MANUAL) {
						// Merge quote row into SP row and write to SP table
						spRow.putAll(msmRow); // TODO Should spRow be sanitised first?
						spCursor.updateCurrentRowFromMap(spRow);
						LOGGER.info("Updated previous quote for symbol {} in SP table: new price={}, timestamp={}", symbol, spRow.get("dPrice"), quoteTime);
						return updateStatus;
					}
					break;
				}
				// Loop
				if (spIt.hasNext()) {
					spRow = spIt.next();
					continue;
				} else {
					break;
				}
			}
		}

		if (highestSpRow.isEmpty()) {
			LOGGER.info("Cannot find quote for symbol {} in SP table with timestamp earlier than new quote timestamp", symbol);
		} else {
			LOGGER.info("Found previous quote for symbol {} in SP table: price={}, hsp={}, timestamp={}", symbol, highestSpRow.get("dPrice"), highestSpRow.get("hsp"), highestSpRow.get("dt"));
		}

		// Add quote row to SP row append list
		hsp++;
		spRow.put("hsp", hsp);
		spRow.put("hsec", hsec);
		spRow.putAll(msmRow); // TODO Should spRow be sanitised first?
		newSpRows.add(spRow);
		LOGGER.info("Added new quote for symbol {} to SP table append list: price={}, hsp={}, timestamp={}", symbol, spRow.get("dPrice"), spRow.get("hsp"), quoteTime);

		return updateStatus;
	}

	public void addNewSpRows() throws IOException {
		if (!newSpRows.isEmpty()) {
			spTable.addRowsFromMaps(newSpRows);
			LOGGER.info("Added {} new {} to SP table from SP table append list", newSpRows.size(), newSpRows.size() == 1 ? "quote" : "quotes");
		}
		return;
	}

	/**
	 * Create a list of investment symbols and corresponding country codes.
	 * 
	 * @param db the MS Money database
	 * @return the list of symbols and corresponding countries
	 * @throws IOException
	 */
	public List<String[]> getSymbols(MsmDb db) throws IOException {
		Map<String, Object> row = null;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> secIt;
		List<String[]> symbols = new ArrayList<String[]>();
		String[] symbol;

		// Build list of symbols + countries
		rowPattern.put("fOLQuotes", true);
		IndexCursor secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		secIt = new IterableBuilder(secCursor).setMatchPattern(rowPattern).forward().iterator();
		while (secIt.hasNext()) {
			symbol = new String[2];
			row = secIt.next();
			if ((symbol[0] = (String) row.get("szSymbol")) != null) {
				symbol[1] = db.getCntryCode((int) row.get("hcntry"));
				symbols.add(symbol);
			}
		}
		return symbols;
	}
}