package uk.co.pueblo.msmcore;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
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

/**
 * A Microsoft Money database instance.
 * 
 * @author Jonathan Casiot
 * @see <a href="https://jackcess.sourceforge.io/apidocs/">Jackcess</a>
 * @see <a href="https://jackcessencrypt.sourceforge.io/apidocs/">Jackcess Encrypt</a>
 */
public class MsmDb {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmDb.class);
	private static final String DHD_TABLE = "DHD";
	private static final String CLI_DAT_TABLE = "CLI_DAT";
	private static final String CNTRY_TABLE = "CNTRY";

	// Instance variables
	private final Database db;
	private Table dhdTable = null;
	private Table cliDatTable = null;
	private Table cntryTable = null;

	// DHD table columns
	public enum DhdColumn {
		BASE_CURRENCY("hcrncDef");

		private final String column;

		DhdColumn(String column) {
			this.column = column;
		}

		public String getName() {
			return column;
		}
	}

	// CLI_DAT table rows
	public enum CliDatRow {
		FILENAME(65541, 8, "rgbVal"), OLUPDATE(917505, 7, "dtVal");

		private final int idData;
		private final int oft;
		private final String valCol;

		CliDatRow(int idData, int oft, String valCol) {
			this.idData = idData;
			this.oft = oft;
			this.valCol = valCol;
		}

		public int getIdData() {
			return idData;
		}

		public int getOft() {
			return oft;
		}

		public String getValCol() {
			return valCol;
		}
	}

	/**
	 * @param fileName the name of the Money file
	 * @param password the password for the Money file
	 * @throws IOException
	 */
	public MsmDb(String fileName, String password) throws IOException {

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
	 * Gets the value of a column in the DHD table.
	 * 
	 * @param dhdCol the name of the column
	 * @return the column value
	 * @throws IOException
	 */
	public int getDhdVal(String dhdCol) throws IOException {
		if (dhdTable == null) {
			dhdTable = db.getTable(DHD_TABLE);
		}
		Row row = dhdTable.getNextRow();
		return (int) row.get(dhdCol);
	}

	/**
	 * Updates a value in the CLI_DAT table.
	 * 
	 * @param name   the name of the row to be updated
	 * @param newVal the new value
	 * @return true if successful, otherwise false
	 * @throws IOException
	 */
	public boolean updateCliDatVal(CliDatRow name, Object newVal) throws IOException {
		if (cliDatTable == null) {
			cliDatTable = db.getTable(CLI_DAT_TABLE);
		}
		IndexCursor cursor = CursorBuilder.createCursor(cliDatTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("idData", name.getIdData()));
		if (found) {
			Column cliDatCol = cliDatTable.getColumn("oft");
			cursor.setCurrentRowValue(cliDatCol, name.getOft());
			cliDatCol = cliDatTable.getColumn(name.getValCol());
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
		if (cntryTable == null) {
			cntryTable = db.getTable(CNTRY_TABLE);
		}
		IndexCursor cursor = CursorBuilder.createCursor(cntryTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("hcntry", hcntry));
		if (found) {
			return (String) cursor.getCurrentRowValue(cntryTable.getColumn("szCode"));
		}
		return null;
	}
}