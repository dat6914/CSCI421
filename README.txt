# CSCI420 - Database System Implementation
# Project - Phase 2
# Group 03

This is a Storage Manager and some basic SQL features.

This implementation includes:
1. Data will be stored to file system as bytes. Opening the data files in
   a text editor should result in mostly unreadable text.
2. use page buffer which is an in-memory buffer to store recently used pages
   when the buffer is full , it will remove and write out the least recently used page
   in order to load the new page to modify.

How to run the program:
    This text is attached with the src folder which contain every Java code files
    Run this command:
            java Main <db loc> <page_size> <buffer_size>

            <db_loc>        the absolute path to the directory to store the database in
            <page_size>     a limited size of a page (unit byte)
            <buffer_size>   the number of pages a buffer can hold

Please check over our Phase 1 again! We have fixed the issues you mentioned before in grading feedback. Thank you!
