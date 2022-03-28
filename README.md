              Index csv files with relational data

For the scenarios where you need to keep the input csv,tsv files intact but need to query those files for auditing using a selection criteria, you will need to build indexes on those files. For example, if you need to gather inventory information for a store and item. If there are a large number of stores and items, this process could be time taking depending on the hardware resources and the type of the system you have.
Following describes a method to do so using python libraries mmap, pandas and SQLITE.
1)	Get the byte positions of a row along with row index using mmap
2)	Use pandas to read the file and the column values where we need to build the index
3)	Load the byte positions along with the key columns and values in SQLITE where you can do additional indexing on the key column values.
4)	Use a procedure to filter using filter criteria. If the filter criteria also includes columns where indexes are available use those if not read all input csv files to do the filter using pandas query.



