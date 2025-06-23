package DBMS;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class DBApp
{
	static int dataPageSize = 2;


	public static void createTable(String tableName, String[] columnsNames)
	{
		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
	}

	
	public static void insert(String tableName, String[] record) {
	    Table t = FileManager.loadTable(tableName);
	    if (t == null) {
	        throw new IllegalArgumentException("Table " + tableName + " does not exist");
	    }

	    // 1) Insert into the table and persist it
	    t.insert(record);
	    FileManager.storeTable(tableName, t);

	    // 2) Update only the columns that actually have a bitmap index
	    List<String> indexedCols = t.getIndexedColumns();
	    for (String idxCol : indexedCols) {
	        BitmapIndex idx = FileManager.loadTableIndex(tableName, idxCol);
	        if (idx == null) continue;

	        // find the position of idxCol in the table schema
	        String[] allCols = t.getColumnNames();
	        int colIndex = -1;
	        for (int i = 0; i < allCols.length; i++) {
	            if (allCols[i].equals(idxCol)) {
	                colIndex = i;
	                break;
	            }
	        }
	        if (colIndex < 0) continue;

	        // append a “1” bit for the new record’s value in this column
	        String val = record[colIndex];
	        idx.appendBit(val, 1);

	        // persist the updated index
	        FileManager.storeTableIndex(tableName, idxCol, idx);
	    }
	}


	public static ArrayList<String []> select(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select();
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(pageNumber, recordNumber);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(cols, vals);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static String getFullTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getFullTrace();
		return res;
	}

	public static String getLastTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getLastTrace();
		return res;
	}
	// Validates records in the specified table by checking if their page files exist.
	 public static ArrayList<String[]> validateRecords(String tableName) {
		// Load the table object from storage using FileManager.  
		 Table table = FileManager.loadTable(tableName);
		 // If the table doesn't exist, return an empty ArrayList to avoid null pointer issues.
	        if (table == null) return new ArrayList<>();
	     // Create a list to collect any missing records
	        ArrayList<String[]> missingRecords = new ArrayList<>();
	     // Get the full trace log string from the table
	        String fullTrace = table.getFullTrace();
	     // Split the trace into individual lines
	        String[] traceLines = fullTrace.split("\n");
	     // Loop through each trace entry
	        for (String traceEntry : traceLines) {
	        	// Check only lines that indicate an inserted record
	            if (traceEntry.startsWith("Inserted:")) {
	                try {
	                    int recordStart = traceEntry.indexOf("[");
	                    int recordEnd = traceEntry.indexOf("]", recordStart);
	                    String recordString = traceEntry.substring(recordStart + 1, recordEnd);
	                    String[] record = recordString.split(",\\s*");
	                 // Extract the page number from the trace string
	                    String pageToken = "at page number:";
	                    int pageIndex = traceEntry.indexOf(pageToken) + pageToken.length();
	                    int commaIndex = traceEntry.indexOf(",", pageIndex);
	                    String pageNumberStr = traceEntry.substring(pageIndex, commaIndex).trim();
	                    int pageNumber = Integer.parseInt(pageNumberStr);
	                 // Build the file path for the page file based on table name and page number
	                    File pageFile = new File(FileManager.directory + "/" + tableName + "/" + pageNumber + ".db");
	                 // If the expected page file does not exist, add the record to the list of missing records
	                    if (!pageFile.exists()) {
	                        missingRecords.add(record);
	                    }
	                } catch (Exception e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	     // Add a line to the trace log indicating how many records were found missing
	        table.addTraceLine("Validating records: " + missingRecords.size() + " records missing.");
	        FileManager.storeTable(tableName, table);
	        return missingRecords;
	    }
	 

	 public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
		    // Load the table object from disk using the table name
		    Table table = FileManager.loadTable(tableName);
		    // If table doesn't exist, exit early
		    if (table == null) return;

		    // List to keep track of page numbers that had records recovered
		    ArrayList<Integer> recoveredPages = new ArrayList<>();

		    // Get the full trace log of operations from the table
		    String fullTrace = table.getFullTrace();
		    // Split the trace log into individual lines
		    String[] traceLines = fullTrace.split("\n");

		    // Index used to keep track of where we are in inserting/recovering records
		    int recordIndex = 0;

		    // Loop through each missing record
		    for (String[] record : missing) {
		        // Initialize page number where the record will be inserted
		        int pageNumber = -1;

		        // Format the record for string matching in trace
		        String recordFormatted = "[" + String.join(", ", record) + "]";

		        // Try to find the page number where this record was previously inserted by checking the trace
		        for (String traceEntry : traceLines) {
		            if (traceEntry.startsWith("Inserted:") && traceEntry.contains(recordFormatted)) {
		                String pageToken = "at page number:";
		                int pageIndex = traceEntry.indexOf(pageToken) + pageToken.length();
		                int commaIndex = traceEntry.indexOf(",", pageIndex);

		                // Extract and parse the page number from the trace string
		                String pageNumberStr = traceEntry.substring(pageIndex, commaIndex).trim();
		                try {
		                    pageNumber = Integer.parseInt(pageNumberStr);
		                } catch (NumberFormatException e) {
		                    // If there's a parsing error, print it and move on
		                    e.printStackTrace();
		                    continue;
		                }
		                break; // Exit the inner loop once we find a match
		            }
		        }

		        // If page number wasn't found in trace, calculate it based on data page size
		        if (pageNumber == -1) {
		            pageNumber = recordIndex / dataPageSize;
		        }

		        // Load the page where the record will be inserted
		        Page page = FileManager.loadTablePage(tableName, pageNumber);
		        // If page doesn't exist yet, create a new one
		        if (page == null) {
		            page = new Page();
		        }

		        // Insert the missing record into the page
		        page.insert(record);

		        // Save the modified page back to disk
		        FileManager.storeTablePage(tableName, pageNumber, page);

		        // Keep track of pages that had recovery done to them (avoid duplicates)
		        if (!recoveredPages.contains(pageNumber)) {
		            recoveredPages.add(pageNumber);
		        }

		        // Update the page count in the table if this is a new page
		        table.updatePageCount(pageNumber + 1);
		        // Increase the total record count in the table
		        table.incrementRecordsCount();

		        // Now we update the bitmap index for this record (for each column)
		        File tableDir = new File(FileManager.directory, tableName);
		        // Get all index files (excluding table and numbered page files)
		        File[] files = tableDir.listFiles((dir, name) -> name.endsWith(".db") && !name.equals(tableName + ".db") && !name.matches("\\d+\\.db"));

		        // If index files exist, update the bitmap index
		        if (files != null) {
		            for (File f : files) {
		                // Get column name from file name
		                String colName = f.getName().replace(".db", "");

		                // Load the bitmap index for this column
		                BitmapIndex index = FileManager.loadTableIndex(tableName, colName);
		                if (index == null) continue;

		                // Find the index of this column in the table
		                int colIndex = -1;
		                String[] colNames = table.getColumnNames();
		                for (int i = 0; i < colNames.length; i++) {
		                    if (colNames[i].equals(colName)) {
		                        colIndex = i;
		                        break;
		                    }
		                }
		                if (colIndex == -1) continue;

		                // Get the value of the column in this record
		                String val = record[colIndex];

		                // Insert the corresponding bit (1 = present) in the bitmap index
		                index.insertBit(val, recordIndex, 1);

		                // Store the updated index back to disk
		                FileManager.storeTableIndex(tableName, colName, index);
		            }
		        }

		        // Move to the next record
		        recordIndex++;
		    }

		    // Log the recovery operation in the trace
		    table.addTraceLine("Recovering " + missing.size() + " records in pages: " + recoveredPages + ".");

		    // Save the updated table metadata and trace
		    FileManager.storeTable(tableName, table);
		}

	 
	 public static void createBitMapIndex(String tableName, String colName) {
	     long startTime = System.currentTimeMillis();
	     Table table = FileManager.loadTable(tableName);
	     if (table == null) {
	         System.out.println("Table " + tableName + " not found.");
	         return;
	     }
	     // Determine the position (index) of the target column
	     int colIndex = -1;
	     String[] columnNames = table.getColumnNames();
	     for (int i = 0; i < columnNames.length; i++) {
	         if (columnNames[i].equals(colName)) {
	             colIndex = i;
	             break;
	         }
	     }
	     if (colIndex == -1) {
	         System.out.println("Column " + colName + " not found.");
	         return;
	     }
	     ArrayList<String[]> records = table.select();
	     BitmapIndex index = new BitmapIndex();
	     // Collect all distinct values in the target column
	     Set<String> distinctValues = new HashSet<>();
	     for (String[] record : records) {
	         distinctValues.add(record[colIndex]);
	     }
	     for (String val : distinctValues) {
	         index.addValue(val);
	     }
	     for (String[] record : records) {
	         String val = record[colIndex];
	         index.appendBit(val, 1);
	     }
	     FileManager.storeTableIndex(tableName, colName, index);
	     long stopTime = System.currentTimeMillis();
	     // Log index-creation event in the table trace
	     table.addTraceLine("Index created for column: " + colName +
	                        ", execution time (mil):" + (stopTime - startTime));
	     // Add the column name to the table’s list of indexed columns
	     table.addIndexedColumn(colName);
	     FileManager.storeTable(tableName, table);
	 }

	 
	 public static String getValueBits(String tableName, String colName, String value) {
		    if (tableName == null || colName == null) {
		        throw new IllegalArgumentException("Table name and column name must not be null.");
		    }

		    Table table = FileManager.loadTable(tableName);
		    int recordCount = (table != null) ? table.select().size() : 0;

		    BitmapIndex index = FileManager.loadTableIndex(tableName, colName);

		    if (index == null) {
		        // Java 8-compatible repeat
		        StringBuilder zeros = new StringBuilder();
		        for (int i = 0; i < recordCount; i++) {
		            zeros.append('0');
		        }
		        return zeros.toString();
		    }

		    String bitStream = index.getBitStream(value);

		    // Pad with 0s if the bitStream is shorter than the total record count
		    if (bitStream.length() < recordCount) {
		        StringBuilder padded = new StringBuilder(bitStream);
		        while (padded.length() < recordCount) {
		            padded.append('0');
		        }
		        bitStream = padded.toString();
		    }

		    return bitStream;
		}
public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
		    // Start timing the execution to log later
		    long startTime = System.currentTimeMillis();

		    // Load the table object from disk
		    Table table = FileManager.loadTable(tableName);
		    // If the table is not found, return an empty result
		    if (table == null) return new ArrayList<>();

		    // Get all records from the table
		    ArrayList<String[]> allRecords = table.select();

		    // Lists to store indexed and non-indexed columns separately
		    ArrayList<String> indexedCols = new ArrayList<>();
		    ArrayList<String> nonIndexedCols = new ArrayList<>();
		    // Map to store column name to its corresponding bitmap index
		    Map<String, BitmapIndex> indexMap = new HashMap<>();

		    // Iterate over each column to check if it has a bitmap index
		    for (String col : cols) {
		        File indexFile = new File(FileManager.directory, tableName + File.separator + col + ".db");
		        // If the index file exists, load it and store in index map
		        if (indexFile.exists()) {
		            BitmapIndex index = FileManager.loadTableIndex(tableName, col);
		            indexMap.put(col, index);
		            indexedCols.add(col);
		        } else {
		            // Otherwise, mark it as non-indexed
		            nonIndexedCols.add(col);
		        }
		    }

		    // Case 1: All columns have indexes
		    if (indexedCols.size() == cols.length) {
		        ArrayList<Integer> resultBitmap = null;

		        // Generate intersection of bitmaps for all indexed columns
		        for (int i = 0; i < cols.length; i++) {
		            String col = cols[i];
		            String val = vals[i];
		            BitmapIndex idx = indexMap.get(col);
		            ArrayList<Integer> bits = idx.getBits(val);

		            if (resultBitmap == null) {
		                resultBitmap = new ArrayList<>(bits);
		            } else {
		                for (int j = 0; j < resultBitmap.size() && j < bits.size(); j++) {
		                    resultBitmap.set(j, resultBitmap.get(j) & bits.get(j));
		                }
		            }
		        }

		        // Filter matching records based on the result bitmap
		        ArrayList<String[]> matched = new ArrayList<>();
		        for (int i = 0; i < resultBitmap.size(); i++) {
		            if (resultBitmap.get(i) == 1 && i < allRecords.size()) {
		                matched.add(allRecords.get(i));
		            }
		        }

		        // Log and return the result
		        long stopTime = System.currentTimeMillis();
		     // sort indexed columns
		        String[] sortedIndexed = indexedCols.toArray(new String[0]);
		        Arrays.sort(sortedIndexed);

		        table.addTraceLine("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
		                ", Indexed columns: " + Arrays.toString(sortedIndexed) +
		                ", Indexed selection count: " + matched.size() +
		                ", Final count: " + matched.size() +
		                ", execution time (mil):" + (stopTime - startTime));

		        FileManager.storeTable(tableName, table);
		        return matched;
		    }

		    // Case 2: Only one column is indexed
		    else if (indexedCols.size() == 1) {
		        String indexedCol = indexedCols.get(0);
		        String indexedVal = null;

		        // Find the corresponding value for the indexed column
		        for (int i = 0; i < cols.length; i++) {
		            if (cols[i].equals(indexedCol)) {
		                indexedVal = vals[i];
		                break;
		            }
		        }

		        // Get matching bitmap and filter records
		        BitmapIndex index = indexMap.get(indexedCol);
		        ArrayList<Integer> bits = index.getBits(indexedVal);
		        ArrayList<String[]> temp = new ArrayList<>();
		        for (int i = 0; i < bits.size(); i++) {
		            if (bits.get(i) == 1 && i < allRecords.size()) {
		                temp.add(allRecords.get(i));
		            }
		        }

		        // Further filter using non-indexed columns
		        ArrayList<String[]> finalResults = new ArrayList<>();
		        for (String[] row : temp) {
		            boolean match = true;
		            for (int i = 0; i < cols.length; i++) {
		                if (cols[i].equals(indexedCol)) continue;
		                String val = vals[i];
		                String[] colNames = table.getColumnNames();
		                for (int j = 0; j < colNames.length; j++) {
		                    if (colNames[j].equals(cols[i]) && !row[j].equals(val)) {
		                        match = false;
		                        break;
		                    }
		                }
		                if (!match) break;
		            }
		            if (match) finalResults.add(row);
		        }

		        // Log and return the result
		        long stopTime = System.currentTimeMillis();
		     // sort indexed (one) and non-indexed columns
		        String[] sortedIndexed    = indexedCols.toArray(new String[0]);
		        String[] sortedNonIndexed = nonIndexedCols.toArray(new String[0]);
		        Arrays.sort(sortedIndexed);
		        Arrays.sort(sortedNonIndexed);

		        table.addTraceLine("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
		                ", Indexed columns: " + Arrays.toString(sortedIndexed) +
		                ", Indexed selection count: " + temp.size() +
		                ", Non Indexed: " + Arrays.toString(sortedNonIndexed) +
		                ", Final count: " + finalResults.size() +
		                ", execution time (mil):" + (stopTime - startTime));

		        FileManager.storeTable(tableName, table);
		        return finalResults;
		    }

		    // Case 3: Some (but not all) columns are indexed
		    else if (indexedCols.size() > 0) {
		        ArrayList<Integer> resultBitmap = null;

		        // Combine indexed column bitmaps
		        for (String indexedCol : indexedCols) {
		            String val = null;
		            for (int i = 0; i < cols.length; i++) {
		                if (cols[i].equals(indexedCol)) {
		                    val = vals[i];
		                    break;
		                }
		            }

		            BitmapIndex idx = indexMap.get(indexedCol);
		            ArrayList<Integer> bits = idx.getBits(val);
		            if (resultBitmap == null) {
		                resultBitmap = new ArrayList<>(bits);
		            } else {
		                for (int i = 0; i < resultBitmap.size() && i < bits.size(); i++) {
		                    resultBitmap.set(i, resultBitmap.get(i) & bits.get(i));
		                }
		            }
		        }

		        // Filter records based on combined bitmap
		        ArrayList<String[]> temp = new ArrayList<>();
		        for (int i = 0; i < resultBitmap.size(); i++) {
		            if (resultBitmap.get(i) == 1 && i < allRecords.size()) {
		                temp.add(allRecords.get(i));
		            }
		        }

		        // Further filter using non-indexed columns
		        ArrayList<String[]> finalResults = new ArrayList<>();
		        for (String[] row : temp) {
		            boolean match = true;
		            for (int i = 0; i < cols.length; i++) {
		                if (indexedCols.contains(cols[i])) continue;
		                String val = vals[i];
		                String[] colNames = table.getColumnNames();
		                for (int j = 0; j < colNames.length; j++) {
		                    if (colNames[j].equals(cols[i]) && !row[j].equals(val)) {
		                        match = false;
		                        break;
		                    }
		                }
		                if (!match) break;
		            }
		            if (match) finalResults.add(row);
		        }

		        // Log and return the result
		        long stopTime = System.currentTimeMillis();
		     // Sort non-indexed columns for consistent trace format
		     // Sort non-indexed columns
		        String[] sortedNonIndexed = nonIndexedCols.toArray(new String[0]);
		        Arrays.sort(sortedNonIndexed);

		        // Sort indexed columns
		        String[] sortedIndexed = indexedCols.toArray(new String[0]);
		        Arrays.sort(sortedIndexed);

		        // Add trace line with sorted arrays
		        table.addTraceLine("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
		                ", Indexed columns: " + Arrays.toString(sortedIndexed) +
		                ", Indexed selection count: " + temp.size() +
		                ", Non Indexed: " + Arrays.toString(sortedNonIndexed) +
		                ", Final count: " + finalResults.size() +
		                ", execution time (mil):" + (stopTime - startTime));


		        FileManager.storeTable(tableName, table);
		        return finalResults;
		    }

		    // Case 4: No columns are indexed (fallback to full scan)
		    else {
		        ArrayList<String[]> finalResults = new ArrayList<>();
		        String[] colNames = table.getColumnNames();

		        // Linear scan of all records to filter based on values
		        for (String[] row : allRecords) {
		            boolean match = true;
		            for (int i = 0; i < cols.length; i++) {
		                String col = cols[i];
		                String val = vals[i];
		                for (int j = 0; j < colNames.length; j++) {
		                    if (colNames[j].equals(col) && !row[j].equals(val)) {
		                        match = false;
		                        break;
		                    }
		                }
		                if (!match) break;
		            }
		            if (match) finalResults.add(row);
		        }

		        // Log and return the result
		        long stopTime = System.currentTimeMillis();
		        String[] sortedCols = cols.clone();
		        Arrays.sort(sortedCols);

		        table.addTraceLine("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals) +
		                           ", Non Indexed: " + Arrays.toString(sortedCols) +
		                           ", Final count: " + finalResults.size() +
		                           ", execution time (mil):" + (stopTime - startTime));

		        FileManager.storeTable(tableName, table);
		        return finalResults;
		    }
		}
	
	public static void main(String []args) throws IOException 
	 { 
	  FileManager.reset(); 
	  String[] cols = {"id","name","major","semester","gpa"}; 
	  createTable("student", cols); 
	  String[] r1 = {"1", "stud1", "CS", "5", "0.9"}; 
	  insert("student", r1); 
	   
	  String[] r2 = {"2", "stud2", "BI", "7", "1.2"}; 
	  insert("student", r2); 
	   
	  String[] r3 = {"3", "stud3", "CS", "2", "2.4"}; 
	  insert("student", r3); 
	   
	  String[] r4 = {"4", "stud4", "CS", "9", "1.2"}; 
	  insert("student", r4); 
	   
	  String[] r5 = {"5", "stud5", "BI", "4", "3.5"}; 
	  insert("student", r5); 
	   
	  //////// This is the code used to delete pages from the table 
	  System.out.println("File Manager trace before deleting pages: "+FileManager.trace()); 
	  String path = 
	FileManager.class.getResource("FileManager.class").toString(); 
	     File directory = new File(path.substring(6,path.length()-17) + 
	File.separator 
	       + "Tables//student" + File.separator); 
	     File[] contents = directory.listFiles(); 
	     int[] pageDel = {0,2}; 
	for(int i=0;i<pageDel.length;i++) 
	{ 
	contents[pageDel[i]].delete(); 
	} 
	////////End of deleting pages code 
	System.out.println("File Manager trace after deleting pages:"+FileManager.trace()); 
	ArrayList<String[]> tr = validateRecords("student"); 
	System.out.println("Missing records count: "+tr.size()); 
	recoverRecords("student", tr); 
	System.out.println("--------------------------------"); 
	System.out.println("Recovering the missing records."); 
	tr = validateRecords("student"); 
	System.out.println("Missing record count: "+tr.size()); 
	System.out.println("File Manager trace after recovering missing records: "+FileManager.trace()); 
	System.out.println("--------------------------------"); 
	System.out.println("Full trace of the table: "); 
	System.out.println(getFullTrace("student")); 
	} 

}
