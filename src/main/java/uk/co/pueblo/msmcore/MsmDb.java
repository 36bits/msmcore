package uk.co.pueblo.msmcore;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.sql.SQLException;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;
import com.healthmarketscience.jackcess.util.OleBlob;
import com.healthmarketscience.jackcess.util.OleBlob.Builder;

/**
 * A Microsoft Money database instance.
 * 
 * @author Jonathan Casiot
 * @see <a href="https://jackcess.sourceforge.io/apidocs/">Jackcess</a>
 * @see <a href="https://jackcessencrypt.sourceforge.io/apidocs/">Jackcess
 *      Encrypt</a>
 */
public class MsmDb {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmDb.class);
	private static final String DHD_TABLE = "DHD";
	private static final String CLI_DAT_TABLE = "CLI_DAT";
	private static final String CNTRY_TABLE = "CNTRY";

	// Instance variables
	private final Database db;
	private final Table cliDatTable;
	private final Table cntryTable;
	private final Table dhdTable;
	private final Row dhdRow;
	private final byte[] dhdData;

	// CLI_DAT table rows
	public enum CliDatValue {
		FILENAME(65541, "rgbVal"), OLUPDATE(917505, "dtVal");

		private final int idData;
		private final String valCol;

		CliDatValue(int idData, String valCol) {
			this.idData = idData;
			this.valCol = valCol;
		}
	}

	// Indices of values in DHD data blob
	public enum DhdDataValue {
		SEC_NEXT_PK(236), SP_NEXT_PK(260);

		private final int index;

		DhdDataValue(int index) {
			this.index = index;
		}
	}

	/**
	 * @param fileName the name of the Money file
	 * @param password the password for the Money file
	 * @throws IOException
	 * @throws SQLException
	 */
	public MsmDb(String fileName, String password) throws IOException, SQLException {

		// Create lock file
		final String lockFileName;
		final int i = fileName.lastIndexOf('.');
		if (i <= 0) {
			lockFileName = fileName;
		} else {
			lockFileName = fileName.substring(0, i);
		}
		final File lockFile = new File(lockFileName + ".lrd");
		LOGGER.info("Creating lock file: {}", lockFile.getAbsolutePath());
		if (!lockFile.createNewFile()) {
			throw new FileAlreadyExistsException("Lock file already exists");
		}
		lockFile.deleteOnExit();

		// Open Money database
		final File dbFile = new File(fileName);
		final CryptCodecProvider cryptCp;

		if (password.isEmpty()) {
			cryptCp = new CryptCodecProvider();
		} else {
			cryptCp = new CryptCodecProvider(password);
		}
		LOGGER.info("Opening Money file: {}", dbFile.getAbsolutePath());
		db = new DatabaseBuilder(dbFile).setCodecProvider(cryptCp).open();
		db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

		// Open the core tables
		cliDatTable = db.getTable(CLI_DAT_TABLE);
		cntryTable = db.getTable(CNTRY_TABLE);
		dhdTable = db.getTable(DHD_TABLE);

		// Get the DHD row and data blob
		dhdRow = dhdTable.getNextRow();
		OleBlob dhdBlob = dhdRow.getBlob("rgbNhdata");
		dhdData = dhdBlob.getBytes(1, (int) dhdBlob.length());
		dhdBlob.close();

		return;
	}

	/**
	 * Gets the Jackcess database instance.
	 * 
	 * @return the Jackcess database instance
	 */
	public Database getDb() {
		return db;
	}

	/**
	 * Closes the Jackcess database instance.
	 */
	public void closeDb() throws IOException {
		LOGGER.info("Closing Money file: {}", db.getFile());
		db.close();
		return;
	}

	/**
	 * Gets an integer value from the DHD row.
	 * 
	 * @param dhdCol the name of the column
	 * @return the column value
	 * @throws IOException
	 */
	public int getDhdInt(String dhdCol) throws IOException {
		return (int) dhdRow.get(dhdCol);
	}

	/**
	 * Gets an integer value from the DHD data blob.
	 * 
	 * @param ddVal the ENUM of the value
	 * @return the value
	 */
	public int getDhdDataInt(DhdDataValue ddVal) {
		int value = 0;
		for (int i = ddVal.index + Integer.BYTES - 1; i >= ddVal.index; i--) {
			value = (value << 8) + (dhdData[i] & 0xFF);
		}
		return value;
	}

	/**
	 * Sets an integer value in the DHD data blob.
	 * 
	 * @param ddVal the ENUM of the value
	 * @param value the value to be set
	 * @return void
	 * @throws IOException
	 */
	public void setDhdDataInt(DhdDataValue ddVal, int value) throws IOException {
		for (int i = ddVal.index; i < ddVal.index + Integer.BYTES; i++) {
			dhdData[i] = (byte) (value & 0xFF);
			value >>= 8;
		}

		// Write DHD data blob to DHD table
		OleBlob dhdBlob = Builder.fromInternalData(dhdData);
		dhdRow.put("rgbNhdata", dhdBlob);
		dhdTable.updateRow(dhdRow);
		dhdBlob.close();
		return;
	}

	/**
	 * Updates a value in the CLI_DAT table.
	 * 
	 * @param name   the name of the row to be updated
	 * @param newVal the new value
	 * @return true if successful, otherwise false
	 * @throws IOException
	 */
	public boolean updateCliDatVal(CliDatValue name, Object newVal) throws IOException {
		IndexCursor cursor = CursorBuilder.createCursor(cliDatTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("idData", name.idData));
		if (found) {
			Column cliDatCol = cliDatTable.getColumn(name.valCol);
			cursor.setCurrentRowValue(cliDatCol, newVal);
			return true;
		}
		return false;
	}

	/**
	 * Gets the two-character country code for a hcntry from the CNTRY table.
	 * 
	 * @param hcntry the hcntry to find the country code for
	 * @return the country code, or null if not found
	 * @throws IOException
	 */
	public String getCntryCode(int hcntry) throws IOException {
		IndexCursor cursor = CursorBuilder.createCursor(cntryTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("hcntry", hcntry));
		if (found) {
			return (String) cursor.getCurrentRowValue(cntryTable.getColumn("szCode"));
		}
		return null;
	}
}