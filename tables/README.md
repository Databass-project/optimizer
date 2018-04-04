## Query Optimizer for CS3223 Database implementations

### Set-up

1. run `source queryenv`
2. run `source build.sh`.
* Note that every time you will need to run `build.sh` to recompile the code, NOT via any IDE such as IntelliJ to build the project.

### Creating tables

- let the new table be named `new_table` from here onward
1. Create `new_table.det` file, specifying its metadata
2. Run `java RandomDB new_table <num_tuples>`. This will create `new_table.txt`, the actual table, and `new_table.md`, serialized schema file.

### Convert table in text file into object file

1. Run `java ConvertTxtToTbl new_table.txt`. This will create `new_table.tbl` and `new_table.stat`

### Run query

1. Run `java QueryMain query.in query.out`
* When you run `QueryMain`, the table text file must be in the current working directory as well.








