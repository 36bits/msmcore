package uk.co.pueblo.msmcore;

import java.io.IOException;
import java.sql.SQLException;
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

import uk.co.pueblo.msmcore.MsmDb.DhdDataValue;

public class MsmSecurity extends MsmInstrument {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmSecurity.class);
	private static final String SEC_TABLE = "SEC";
	private static final String SP_TABLE = "SP";
	private static final int SRC_MANUAL = 5;
	private static final int SRC_ONLINE = 6;
	private static final Properties PROPS;

	// Instance variables
	private final MsmDb msmDb;
	private final Table secTable;
	private final Table spTable;
	private ArrayList<Map<String, Object>> newSpRows = new ArrayList<>();
	private int hsp = 0;

	static {
		Properties props = null;
		try {
			props = loadProperties("MsmSecurity.properties");
		} catch (Exception e) {
			LOGGER.debug("Exception occured!", e);
			LOGGER.fatal("Failed to load properties: {}", e.getMessage());
		}
		PROPS = props;
	}	
	
	// Constructor
	public MsmSecurity(MsmDb msmDb) throws IOException, SQLException {

		this.msmDb = msmDb;
		Database db = msmDb.getDb();

		// Open the securities tables
		secTable = db.getTable(SEC_TABLE);
		spTable = db.getTable(SP_TABLE);

		// Get the next hsp (SP table primary key)
		hsp = msmDb.getDhdDataInt(DhdDataValue.SP_NEXT_PK);
		LOGGER.debug("Next hsp={}", hsp);

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
	 * @return
	 * @throws IOException
	 * @throws MsmInstrumentException
	 */
	public void update(Map<String, String> sourceRow) throws IOException, MsmInstrumentException {
		
		updateStatus = UpdateStatus.OK;

		Map<String, String> validatedRow = new HashMap<>(validateQuoteRow(sourceRow, PROPS)); // validate incoming row
		Map<String, Object> msmRow = new HashMap<>(buildMsmRow(validatedRow, PROPS)); // now build MSM row

		String symbol = msmRow.get("xSymbol").toString();
		String quoteType = validatedRow.get("xType").toString();
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
			incSummary(quoteType, UpdateStatus.NOT_FOUND);
			throw new MsmInstrumentException("Cannot find symbol " + symbol + " in SEC table");
		}

		// Update SEC table
		LocalDateTime quoteTime;
		long quoteAgeDays = 0;
		if (msmRow.containsKey("dtLastUpdate")) {
			quoteTime = (LocalDateTime) msmRow.get("dtLastUpdate");
			if (!quoteTime.equals((LocalDateTime) secRow.get("dtLastUpdate"))) {
				// Merge quote row into SEC row and write to SEC table
				secRow.putAll(msmRow); // TODO Should secRow be sanitised first?
				secCursor.updateCurrentRowFromMap(secRow);
				LOGGER.info("Updated SEC table for symbol {}", symbol);
			} else if ((quoteAgeDays = ChronoUnit.DAYS.between(quoteTime, LocalDateTime.now())) > Long.parseLong(PROPS.getProperty("quote.staledays"))) {
				// Quote data is stale
				updateStatus = UpdateStatus.STALE;
			} else {
				// Skip update
				incSummary(quoteType, UpdateStatus.NO_CHANGE);
				LOGGER.info("Skipped update for symbol {}, new quote has same timestamp as previous quote: timestamp={}", symbol, quoteTime);
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
			// long firstDaysDiff = Math.abs(Duration.between(firstDate,
			// quoteDate).toDays());
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
						if (updateStatus == UpdateStatus.STALE) {
							if ((double) spRow.get("dChange") == 0) {
								incSummary(quoteType, updateStatus);
								LOGGER.warn("Skipped update for symbol {}, received stale quote data: timestamp={}, age days={}", symbol, quoteTime, quoteAgeDays);
								return;
							} else {
								LOGGER.warn("Received new stale quote data for symbol {}, setting change value in SP table to zero: timestamp={}, age days={}", symbol, quoteTime, quoteAgeDays);
								msmRow.put("dChange", 0);
								updateStatus = UpdateStatus.NEW_STALE;
							}
						}
						// Merge quote row into SP row and write to SP table
						spRow.putAll(msmRow); // TODO Should spRow be sanitised first?
						spCursor.updateCurrentRowFromMap(spRow);
						incSummary(quoteType, updateStatus);
						LOGGER.info("Updated previous quote for symbol {} in SP table: new price={}, timestamp={}", symbol, spRow.get("dPrice"), quoteTime);
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

		if (highestSpRow.isEmpty()) {
			LOGGER.info("Cannot find quote for symbol {} in SP table with timestamp earlier than new quote timestamp", symbol);
		} else {
			LOGGER.info("Found previous quote for symbol {} in SP table: price={}, hsp={}, timestamp={}", symbol, highestSpRow.get("dPrice"), highestSpRow.get("hsp"), highestSpRow.get("dt"));
		}

		// Add quote row to SP row append list
		spRow.put("hsp", hsp);
		spRow.put("hsec", hsec);
		spRow.putAll(msmRow); // TODO Should spRow be sanitised first?
		newSpRows.add(spRow);
		incSummary(quoteType, updateStatus);
		LOGGER.info("Added new quote for symbol {} to SP table append list: price={}, hsp={}, timestamp={}", symbol, spRow.get("dPrice"), hsp++, quoteTime);
		return;
	}

	public void addNewRows() throws IOException, SQLException {
		if (!newSpRows.isEmpty()) {
			spTable.addRowsFromMaps(newSpRows);
			LOGGER.info("Added {} new {} to SP table from SP table append list, total SP table rows={}", newSpRows.size(), newSpRows.size() == 1 ? "quote" : "quotes", spTable.getRowCount());
			msmDb.setDhdDataInt(DhdDataValue.SP_NEXT_PK, hsp);
		}
		return;
	}
}