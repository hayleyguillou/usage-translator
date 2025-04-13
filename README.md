# usage-translator
Take-home coding assessment for NetNation

## Setup Instructions

### Install Dependencies
Ensure you have Python 3.11 or later installed. Then, create a virtual environment and install the required dependencies:

```bash
pip install -r requirements.txt
```

## Running the Program
The program can be run from the command line using the following arguments:

- `--csv`: Path to the CSV report file (default: `data/Sample_Report.csv`).
- `--json`: Path to the typemap JSON file (default: `data/typemap.json`).
- `--batch-insert-size`: Batch insert size for SQL queries (default: `0` for no batching).
- `--log`: If set, logs will also be written to the default log file (`usage_translator.log`).

```bash
python usage_translator.py --csv path/to/report.csv --json path/to/typemap.json --batch-insert-size 100 --log
```

### Tests

Unit tests are in the `test/` folder. Most recent testing output saved in `pytest_output.txt`. Use the following command to run all tests:

```
pytest
```

### Sample Output

Insert Statements:

```
output/chargeable_insert_rows.sql
output/domains_insert_rows.sql
```

Logs:

```
usage_translator.log
```

Test output:

```
pytest_output.txt
```
___

### Summary

- Successfully generates SQL `INSERT` statements for the `chargeable` and `domains` tables based on the provided CSV and JSON inputs.
- Logs a summary of usage by product.
- Implements optional batching functionality to prevent potential timeouts during large SQL inserts.
- Space/Time Complexity: both are **O(n + k)** where n is the number of rows in the CSV and k is the size of the JSON file.

#### **Design Decisions**
1. Direct Writing vs. In-Memory Storage
    - SQL statements are written directly to the output file to reduce memory usage.
2. Batching 
    - Optional functioality to optimize db performance and avoid timeouts during large inserts.


#### **Future Improvements**
1. Performance/Scalability:
   - Implement streaming to process CSV file without loading it entirely into memory.
   - Use parallelization to process rows concurrently.

2. Maintainability:
   - Refactor `processor.py` into a class-based design to encapsulate logic and enable easier testing and extension.
   - Create a dedicated class for batching logic to reduce code duplication.
   - Add more options for dry run and logging level and summary statistics.

3. Testing:
   - Add integration tests to validate end-to-end workflow.
