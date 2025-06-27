#  Java-DBMS | CSEN604 – Custom DBMS in Java

A lightweight **custom Database Management System (DBMS)** built in **Java** for the **CSEN604 course** at the German University in Cairo.  
This project implements core database functionalities including **record storage**, **indexing**, and **recovery**, without using any external DB libraries.

---

##  Folder Structure

```
src/
└── DBMS/
    ├── BitmapIndex.java        # Helper class for bitmap indexing
    ├── DBApp.java              # Core logic for DBMS operations
    ├── FileManager.java        # Handles serialization, storage, recovery
    ├── Page.java               # Page-level abstraction for records
    ├── Table.java              # Structure and behavior of tables
    ├── DBAppTestsMS2.java      # Instructor-provided test file
    ├── MS2_Tests_01.java       # Custom test cases
    ├── MS2_Tests_02.java
    ├── MS2_Tests_03.java
    ├── MS2_Tests_04.java
    ├── MS2_Tests_05.java
    ├── MS2_Tests_06.java
    ├── MS2_Tests_07.java
    ├── MS2_Tests_08.java
    ├── MS2_Tests_09.java
    ├── MS2_Tests_10.java
    └── MS2_Tests_11.java
```

---

##  Features

###  Milestone 1 (MS1)
- Table creation with custom column names
- Row insertion with automatic page management
- Full table selection (`SELECT *`)
- Conditional selection by column values (`WHERE`)
- Direct access using page and row position
- Operation tracing (full and last action)
- File-based table/page serialization via `FileManager`

###  Milestone 2 (MS2)
- Bitmap indexing on selected columns
- Index-based conditional selection (fully, partially, or non-indexed)
- Bitstream retrieval for specific values
- Page-level data recovery after simulated page loss
- Updated trace logging for all new functionalities

---

##  Sample Test Case

```java
String[] cols = {"id", "name", "major", "semester", "gpa"};
DBApp.createTable("student", cols);

DBApp.insert("student", new String[]{"1", "stud1", "CS", "5", "0.9"});
DBApp.insert("student", new String[]{"2", "stud2", "BI", "7", "1.2"});

DBApp.createBitMapIndex("student", "major");
System.out.println(DBApp.getValueBits("student", "major", "CS"));
```

---

##  Learning Outcomes

Through this project, we gained hands-on experience in:

- Simulating real DBMS architecture (tables, pages, records)
- Designing serialization and persistent storage using Java
- Implementing and applying bitmap indexing for performance
- Building custom selection and record tracing tools
- Executing page-level recovery mechanisms
- Structuring modular, testable backend Java code

---


