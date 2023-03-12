# CSCI420 - Database System Implementation
# Project - Phase 2
# Group 03

This is a Storage Manager and some basic SQL features.

How to run the program:

    Setup:
    Java SDK: Oracle OpenJDK 19.0.2
    Language level: 19

    1. Make sure that the Java SDK is installed on your system and the path is set correctly.
       You can check this by typing java -version in the terminal.

    2. Open the terminal and enter the src directory.

    3. Compile via this command: 'javac -d bin -sourcepath src -cp lib/* src/Main.java'

    Run program via this command:
            java -cp bin Main <db_loc> <page_size> <buffer_size>

            <db_loc>        the absolute path to the directory to store the database in
            <page_size>     a limited size of a page (unit byte)
            <buffer_size>   the number of pages a buffer can hold

NOTE: Please check over our Phase 1 again! We have fixed the issues you mentioned before in grading feedback. Thank you!
