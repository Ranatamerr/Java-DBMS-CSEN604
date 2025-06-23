package DBMS;

import java.io.Serializable;
import java.util.*;

public class BitmapIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    // value -> list of bits (0 or 1)
    private Map<String, ArrayList<Integer>> index;
    private int recordCount; // Track total records
    

    public BitmapIndex() {
        this.index = new HashMap<>();
        this.recordCount = 0;
    }

    // Initialize a new entry for a column value
    public void addValue(String value) {
        index.putIfAbsent(value, new ArrayList<>(Collections.nCopies(recordCount, 0)));
    }

    // Append 0 or 1 to the bitmap of a specific value
    public void appendBit(String value, int bit) {
    	// Pad all existing bitstreams to recordCount
        for (String key : index.keySet()) {
            ArrayList<Integer> bits = index.get(key);
            while (bits.size() < recordCount) {
                bits.add(0);
            }
        }
        // Add or update the value's bitstream
        index.computeIfAbsent(value, k -> new ArrayList<>(Collections.nCopies(recordCount, 0)));
        index.get(value).add(bit);
        // Pad other values with 0
        for (String key : index.keySet()) {
            if (!key.equals(value)) {
                index.get(key).add(0);
            }
        }
        recordCount++;
    }

    // Return the bitstream as a string (e.g. "101")
    public String getBitStream(String value) {
    	ArrayList<Integer> bits = index.getOrDefault(value, new ArrayList<>(Collections.nCopies(recordCount, 0)));
        StringBuilder result = new StringBuilder();
        for (int b : bits) {
            result.append(b);
        }
        // Pad with zeros to match recordCount
        while (result.length() < recordCount) {
            result.append('0');
        }
        return result.toString();
    }


    // Optional: Access raw bit list (e.g., for AND operations)
    public ArrayList<Integer> getBits(String value) {
    	ArrayList<Integer> bits = index.getOrDefault(value, new ArrayList<>(Collections.nCopies(recordCount, 0)));
        while (bits.size() < recordCount) {
            bits.add(0);
        }
        return bits;
    }

    // Return all values (i.e., keys in the index)
    public Set<String> getValues() {
        return index.keySet();
    }
    
    public void insertBit(String value, int position, int bit) {
        // Check if the insertion position is valid (i.e., does not exceed current record count)
        if (position > recordCount) {
            throw new IllegalArgumentException("Position " + position + " exceeds record count " + recordCount);
        }

        // Loop through all existing keys (values) in the bitmap index
        for (String key : index.keySet()) {
            ArrayList<Integer> bits = index.get(key); // Get the bit list for this value

            // Ensure the list is large enough to support insertion at the given position
            while (bits.size() <= position) {
                bits.add(0); // Fill with 0s if needed
            }

            // Insert the new bit at the specified position
            // Set to the provided bit if this key matches the value being inserted; otherwise insert 0
            bits.add(position, key.equals(value) ? bit : 0);

            // Ensure the list has length at least recordCount + 1 after the insertion
            while (bits.size() < recordCount + 1) {
                bits.add(0);
            }
        }

        // If the value is not already in the index, create a new bit list for it
        index.computeIfAbsent(value, k -> {
            // Initialize with 0s, one longer than current recordCount
            ArrayList<Integer> bits = new ArrayList<>(Collections.nCopies(recordCount + 1, 0));
            bits.set(position, bit); // Set the bit at the correct position
            return bits;
        });

        // Increment the total record count since a new record has been inserted
        recordCount++;
    }


    // Optional: Print the entire bitmap index for debugging
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String val : index.keySet()) {
            sb.append(val).append(": ").append(getBitStream(val)).append("\n");
        }
        return sb.toString();
    }
}
