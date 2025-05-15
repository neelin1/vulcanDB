# VulcanDB

<!-- MAIN BODY -->

## Installation

Before you start the installation process make sure you have python installed.

1. Clone this repositor on your local machine:

```bash
git clone git@github.com:vulcanDB/vulcanDB.git
```

2. Move inside the main project directory:

```bash
cd vulcanDB
```

3. Setup and activate your virtual environment (optional):

```bash
# To create a virtual env:
python -m venv .venv    # For Windoes & Linux
python3  -m venv .venv  # If you're on MacOS

# For activation use one of the following commands based on your OS:
`source .venv/bin/activate`   # On Mac / Linux
.venv\Scripts\activate.bat  # In Windows CMD
.venv\Scripts\Activate.ps1  # In Windows Powershel
```

4 Install the required packages from the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

## Setting Up Your Application

Before launching the application, follow these steps to configure your environment:

### Create `.env` file

1. Duplicate the provided `example.env` file.
2. Rename the duplicated file to `.env`.
3. Open the `.env` file and insert:

```
OPENAI_API_KEY=sk-..
```

## Usage

Here's how you can use Vulcan:

From the root directory execute the following command:

```bash
python -m vulcan -f <input_file_name> --db_uri <postgres_uri>
```

Replace `<input_file_name>` with the name of the csv for which you want to generate SQL queries.

Replace `<input_file_name>` with the name of the CSV for which you want to generate SQL queries.

<postgres_uri> will be the in format: `postgresql://<username>:<password>@<host>/<database_name>`

### Intructions on creating a local PostgreSQL database (Optional)

If you don't already have a PostgreSQL database, here's how to create one locally:

1. **Install PostgreSQL** (if not already installed):

   - **macOS** (with Homebrew):
     ```bash
     brew install postgresql
     brew services start postgresql
     ```
   - **Ubuntu**:
     ```bash
     sudo apt update
     sudo apt install postgresql postgresql-contrib
     sudo service postgresql start
     ```
   - **Windows**: Download from [https://www.postgresql.org/download/windows/](https://www.postgresql.org/download/windows/)

2. **Create a user and database**:
   Open a terminal and run:

   ```bash
   psql postgres
   ```

   Then inside the `psql` prompt:

   ```sql
   CREATE USER vulcan_user WITH PASSWORD 'somePassword';
   CREATE DATABASE vulcandb OWNER vulcan_user;
   GRANT ALL PRIVILEGES ON DATABASE vulcandb TO vulcan_user;
   \q
   ```

3. **Construct your connection URI**:
   The URI format is:
   ```
   postgresql://<username>:<password>@<host>/<database>
   ```
   Example:
   ```
   postgresql://vulcan_user:somePassword@localhost/vulcandb
   ```

You can now use this URI with VulcanDB.

### Options

VulcanDB utilizes the following command-line arguments to customize its behavior:

- `-f FILE_NAME, --file_name FILE_NAME`: **(Required)** File name containing SQL queries. This is the path to your input CSV file.
- `--db_uri DB_URI`: **(Required)** Path to the database file. This should be a valid PostgreSQL connection URI, e.g., `postgresql://user:password@host/database`.
- `--single_table`: **(Optional)** Force generation of a single table schema. If omitted, Vulcan attempts might create a multi-table schema.

To view a comprehensive list of all available command-line options and their detailed descriptions, you can run the following command from the project's root directory:

```bash
python -m vulcan -h
```

This command will output the help message, which looks like this:

```bash
usage: python -m vulcan [-h] [-f FILE_NAME] [--db_uri DB_URI] [--single_table]

Process some arguments.

optional arguments:
  -h, --help            show this help message and exit
  -f FILE_NAME, --file_name FILE_NAME
                        File name containing SQL queries
  --db_uri DB_URI       Path to the database file
  --single_table        Force generation of a single table schema (default:
                        attempts multi-table)
```

## Tests

To execute tests you can use the following command:

```bash
pytest
```
