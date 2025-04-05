# CS 122A ZotStreaming
This course project is a command-line program allowing you to manage a MySQL database for a hypothetical video streaming platform.
## How to Use
1. At the top of project.py, replace the MySQL server credentials with your own.
2. Make sure you have installed MySQL Connector. You can do this by running the command:

```pip install mysql-connector-python```
## Example Usage
You can create a database from the provided test data using the command:

```python project.py import test_data```

Or, you can use your own folder of CSV files as long as the file names, columns, etc. are the same as for the test data.


For a list of all 12 functions you can use to interact with the database, run:

```python project.py --help```

All commands will be of the form ```<function name> [param1] [param2] ...```
