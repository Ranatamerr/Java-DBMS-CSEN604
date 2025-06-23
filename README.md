Java-DBMS-CSEN604
Custom database management system implemented in Java for CSEN604 project

>> Features

> Milestone 1 (MS1)
- Table creation with custom column names
- Row insertion with automatic page management
- Full table selection (`SELECT *`)
- Conditional selection by column values (`WHERE`)
- Direct access via page and row position
- Operation tracing (full and last action)
- File-based table/page serialization via `FileManager`

> Milestone 2 (MS2)
- Bitmap Indexing on selected columns
- Index-based conditional selection (fully, partially, or non-indexed)
- Bitstream retrieval per value
- Page-level data recovery after simulated page loss
- Updated tracing for all new functionalities

---

> Sample Test Case

```java
String[] cols = {"id", "name", "major", "semester", "gpa"};
DBApp.createTable("student", cols);
DBApp.insert("student", new String[]{"1", "stud1", "CS", "5", "0.9"});
DBApp.insert("student", new String[]{"2", "stud2", "BI", "7", "1.2"});

DBApp.createBitMapIndex("student", "major");
System.out.println(DBApp.getValueBits("student", "major", "CS"));

> Folder Structure
├── DBApp.java               # Core logic for DBMS
├── FileManager.java         # Handles serialization, storage, recovery
├── DBAppTestsMS2.java       # Test suite (provided by instructors)
├── BitMapIndex.java         # Optional helper class for bitmap indexing
├── Tables/                  # Serialized tables & indexes (auto-created)
└── README.md                # You’re reading it!


>Learning Outcomes
Through this project, we gained hands-on experience in:
Data storage architecture
Serialization and persistence
Indexing techniques (bitmap)
Low-level DBMS logic (pages, records)
Recovery and fault-tolerant systems




