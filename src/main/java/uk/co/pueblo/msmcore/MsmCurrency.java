package uk.co.pueblo.msmcore;

import java.io.IOException;
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

public class MsmCurrency extends MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmCurrency.class);
	private static final String CRNC_TABLE = "CRNC";
	private static final String FX_TABLE = "CRNC_EXCHG";
	private static final Properties PROPS = loadProperties("MsmCurrency.properties");

	// Instance variables
	private final Table crncTable;
	private final Table fxTable;

	// Constructor
	public MsmCurrency(MsmDb msmDb) throws IOException {

		Database db = msmDb.getDb();
		int defHcrnc = msmDb.getDhdInt("hcrncDef"); // get the base currency

		// Open the currency tables
		crncTable = db.getTable(CRNC_TABLE);
		fxTable = db.getTable(FX_TABLE);

		// Build list of currency pairs
		Map<String, Object> row = null;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> crncIt;
		String defIsoCode = null;
		rowPattern.put("fOnline", true); // online update flag set
		rowPattern.put("fHidden", false);
		IndexCursor cursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		crncIt = new IterableBuilder(cursor).setMatchPattern(rowPattern).forward().iterator();
		while (crncIt.hasNext()) {
			row = crncIt.next();
			if ((int) row.get("hcrnc") == defHcrnc) {
				defIsoCode = (String) row.get("szIsoCode");
				LOGGER.info("Base currency is {}, hcrnc={}", defIsoCode, defHcrnc);
			} else {
				msmSymbolsCheck.add(row.get("szIsoCode").toString());
			}
		}
		for (int i = 0; i < msmSymbolsCheck.size(); i++) {
			String symbol = defIsoCode + msmSymbolsCheck.get(i) + "=X"; // MSM currency pair pseudo-symbol is FOOBAR=X
			msmSymbolsCheck.set(i, symbol);
			msmSymbols.add(new String[] { symbol });
		}
	}

	/**
	 * Updates the exchange rate for a currency pair.
	 * 
	 * @param sourceRow a row containing the currency quote data to update
	 * @throws IOException
	 * @throws MsmInstrumentException
	 */
	public void update(Map<String, String> sourceRow) throws IOException, MsmInstrumentException {
		
		updateStatus = UpdateStatus.OK;

		Map<String, String> validatedRow = new HashMap<>(validateQuoteRow(sourceRow, PROPS)); // validate incoming row
		Map<String, Object> msmRow = new HashMap<>(buildMsmRow(validatedRow, PROPS)); // now build MSM row

		String symbol = msmRow.get("xSymbol").toString();
		LOGGER.info("Updating exchange rate for symbol {}", symbol);

		// Get hcrncs of currency pair
		int[] hcrnc = { 0, 0 };
		hcrnc[0] = getHcrnc(symbol.substring(0, 3));
		hcrnc[1] = getHcrnc(symbol.substring(3, 6));

		// Update exchange rate
		String quoteType = msmRow.get("xType").toString();
		double newRate = (double) msmRow.get("rate");
		Map<String, Object> fxRowPattern = new HashMap<>();
		IndexCursor fxCursor = CursorBuilder.createCursor(fxTable.getPrimaryKeyIndex());
		Map<String, Object> fxRow = null;
		double oldRate = 0;
		int i;
		for (i = 0; i < 2; i++) {
			fxRowPattern.put("hcrncFrom", hcrnc[i]);
			fxRowPattern.put("hcrncTo", hcrnc[(i + 1) % 2]);
			if (fxCursor.findFirstRow(fxRowPattern)) {
				fxRow = fxCursor.getCurrentRow();
				oldRate = (double) fxRow.get("rate");
				if (i == 1) {
					// Reversed rate
					newRate = 1 / newRate;
					msmRow.put("rate", newRate);
				}
				LOGGER.info("Found exchange rate: from hcrnc={}, to hcrnc={}", hcrnc[i], hcrnc[(i + 1) % 2]);
				if (oldRate != newRate) {
					// Merge quote row into FX row and write to FX table
					fxRow.putAll(msmRow); // TODO Should fxRow be sanitised first?
					fxCursor.updateCurrentRowFromMap(fxRow);
					incSummary(quoteType, updateStatus);
					LOGGER.info("Updated exchange rate: new rate={}, previous rate={}", newRate, oldRate);
					return;
				} else {
					incSummary(quoteType, UpdateStatus.NO_CHANGE);
					LOGGER.info("Skipped update for symbol {}, rate has not changed: new rate={}, previous rate={}", symbol, newRate, oldRate);
					return;
				}
			}
		}
		incSummary(quoteType, UpdateStatus.NOT_FOUND);
		throw new MsmInstrumentException("Cannot find previous exchange rate");
	}

	/**
	 * Gets the hcrnc of a currency from the ISO code.
	 * 
	 * @param isoCode the ISO code to be found
	 * @return the corresponding hcrnc, or -1 if not found
	 * @throws IOException
	 */
	int getHcrnc(String isoCode) throws IOException {
		int hcrnc = -1;
		IndexCursor cursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("szIsoCode", isoCode));
		if (found) {
			hcrnc = (int) cursor.getCurrentRowValue(crncTable.getColumn("hcrnc"));
			LOGGER.info("Found currency {}, hcrnc={}", isoCode, hcrnc);
		} else {
			LOGGER.warn("Cannot find currency {}", isoCode);
		}
		return hcrnc;
	}
}