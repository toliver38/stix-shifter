# PostgreSQL Table Setup

A Python script that creates a PostgreSQL database and populates a table with sample data from a [CSV file](data.csv).

To run the script: 
1. Edit the field names, data types, and data in the `data.csv` file.
2. Run the `setup.py` script with the following parameters:
    
    * Connection to the PostgreSQL instance with host, user, and password.
    * Name of the target database. If the database already exists, it will be dropped and recreated.
    * Name of the table you wish to create and populate.

    ```bash
    python setup.py '{"host": "<host>", "user": "<user>", "password": "<password>"}' "database_name" "<table_name>"
    ```

For your populated table to be used with the [PostgreSQL connector](https://github.com/opencybersecurityalliance/stix-shifter/tree/develop/stix_shifter_modules/postgresql), the [STIX mappings](https://github.com/opencybersecurityalliance/stix-shifter/tree/develop/stix_shifter_modules/postgresql/stix_translation/json) must match the table fields. See the stix-shifter [developer guide](https://github.com/opencybersecurityalliance/stix-shifter/blob/develop/adapter-guide/develop-translation-module.md) for more information on STIX mappings.
