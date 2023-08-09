package uk.co.pueblo.msmcore;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

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
	private static final String SEC_TABLE = "SEC";
	private static final String SP_TABLE = "SP";
	private static final int SRC_MANUAL = 5;
	private static final int SRC_ONLINE = 6;
	private static final Properties PROPS = openProperties("MsmSecurity.properties");

	// Instance variables
	private final Table secTable;
	private final Table spTable;
	private ArrayList<Map<String, Object>> newSpRows = new ArrayList<>();
	private int hsp = 0;

	// Constructor
	public MsmSecurity(MsmDb msmDb) throws IOException {

		Database db = msmDb.getDb();

		// Open the securities tables
		secTable = db.getTable(SEC_TABLE);
		spTable = db.getTable(SP_TABLE);

		// Get current hsp (SP table index)
		IndexCursor spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());
		spCursor.afterLast();
		if (spCursor.getPreviousRow() != null) {
			hsp = (int) spCursor.getCurrentRowValue(spTable.getColumn("hsp"));
		}
		LOGGER.debug("Current highest hsp={}", hsp);

		// Build lists of security symbols and corresponding country codes
		Map<String, Object> row = null;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> secIt;
		rowPattern.put("fOLQuotes", true);
		IndexCursor secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		secIt = new IterableBuilder(secCursor).setMatchPattern(rowPattern).forward().iterator();
		String secSymbol;
		while (secIt.hasNext()) {
			row = secIt.next();
			if ((secSymbol = (String) row.get("szSymbol")) != null) {
				msmSymbolsCheck.add(secSymbol);
				msmSymbols.add(new String[] { secSymbol, msmDb.getCntryCode((int) row.get("hcntry")) });
			}
		}
	}

	/**
	 * Updates the SEC and SP tables with the supplied quote row.
	 * 
	 * @param sourceRow the row containing the quote data to update
	 * @throws IOException
	 */
	public void update(Map<String, String> sourceRow) throws IOException {

		// Validate incoming row
		workingStatus = UpdateStatus.OK;
		Map<String, String> validatedRow = new HashMap<>(validateQuoteRow(sourceRow, PROPS));
		String quoteType = validatedRow.get("xType").toString();
		if (workingStatus == UpdateStatus.ERROR) {
			incSummary(quoteType);
			return;
		}

		// Now build MSM row
		Map<String, Object> msmRow = new HashMap<>(buildMsmRow(validatedRow, PROPS));
		if (workingStatus == UpdateStatus.ERROR) {
			incSummary(quoteType);
			return;
		}

		String symbol = msmRow.get("xSymbol").toString();
		LOGGER.info("Updating quote data for symbol {}, quote type={}", symbol, quoteType);

		// Find symbol in SEC table
		int hsec = -1;
		Map<String, Object> secRow = null;
		IndexCursor secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		if (secCursor.findFirstRow(Collections.singletonMap("szSymbol", symbol))) {
			secRow = secCursor.getCurrentRow();
			hsec = (int) secRow.get("hsec");
			LOGGER.info("Found symbol {} in SEC table: sct={}, hsec={}", symbol, secRow.get("sct"), hsec);
		} else {
			workingStatus = UpdateStatus.ERROR;
			LOGGER.log(workingStatus.level, "Cannot find symbol {} in SEC table", symbol);
			incSummary(quoteType);
			return;
		}

		// Update SEC table
		LocalDateTime quoteTime;
		if (msmRow.containsKey("dtLastUpdate")) {
			quoteTime = (LocalDateTime) msmRow.get("dtLastUpdate");
			if (!quoteTime.equals((LocalDateTime) secRow.get("dtLastUpdate"))) {
				// Merge quote row into SEC row and write to SEC table
				secRow.putAll(msmRow); // TODO Should secRow be sanitised first?
				secCursor.updateCurrentRowFromMap(secRow);
				LOGGER.info("Updated SEC table for symbol {}", symbol);
			} else if (ChronoUnit.DAYS.between(quoteTime, LocalDateTime.now()) > Long.parseLong(PROPS.getProperty("quote.staledays"))) {
				// Quote data is stale
				workingStatus = UpdateStatus.STALE;
			} else {
				// Skip update
				LOGGER.info("Skipped update for symbol {}, new quote has same timestamp as previous quote: timestamp={}", symbol, quoteTime);
				workingStatus = UpdateStatus.SKIP;
				incSummary(quoteType);
				return;
			}
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
						if (workingStatus == UpdateStatus.STALE) {
							if ((double) spRow.get("dChange") == 0) {
								LOGGER.info("Skipped update for symbol {}, received quote data is stale: timestamp={}", symbol, quoteTime);
								workingStatus = UpdateStatus.SKIP;
								incSummary(quoteType);
								return;
							} else {
								LOGGER.info("Received stale quote data for symbol {}, setting SP change to zero", symbol);
								msmRow.put("dChange", 0);
							}
						}
						// Merge quote row into SP row and write to SP table
						spRow.putAll(msmRow); // TODO Should spRow be sanitised first?
						spCursor.updateCurrentRowFromMap(spRow);
						LOGGER.info("Updated previous quote for symbol {} in SP table: new price={}, timestamp={}", symbol, spRow.get("dPrice"), quoteTime);
						incSummary(quoteType);
						return;
					}
					break;
				}
				// Loop
				if (spIt.hasNext()) {
					spRow = spIt.next();
					continue;
				}
				break;
			}
		}

		if (highestSpRow.isEmpty())

		{
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

		incSummary(quoteType);
		return;
	}

	public void addNewSpRows() throws IOException {
		if (!newSpRows.isEmpty()) {
			spTable.addRowsFromMaps(newSpRows);
			LOGGER.info("Added {} new {} to SP table from SP table append list", newSpRows.size(), newSpRows.size() == 1 ? "quote" : "quotes");
		}
		return;
	}
}