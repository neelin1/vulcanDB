import pytest
from pglast import parse_sql
from mo_sql_parsing import parse

from vulcan.parsers.query import (
    parse_sql_query,
    extract_columns_from_parsed_query,
    extract_table_constraints_from_parsed_query,
    extract_column_names_from_parsed_query,
    extract_foreign_keys_from_parsed_query,
)

test_cases = [
    {
        "query": 'CREATE TABLE "employees" ("Id" INT, "Name" VARCHAR(100), "ManagerId" INT, FOREIGN KEY ("ManagerId") REFERENCES "Managers"("Id"))',
        "expected_output": {
            "table_name": "employees",
            "columns": ["Id", "Name", "ManagerId"],
            "foreign_keys": ["Managers"],
        },
    },
    {
        "query": 'CREATE TABLE "departments" ("Id" INT, "DepartmentName" VARCHAR(100), FOREIGN KEY ("DepartmentId") REFERENCES "Departments"("Id"), FOREIGN KEY ("ManagerId") REFERENCES "Managers"("Id"))',
        "expected_output": {
            "table_name": "departments",
            "columns": ["Id", "DepartmentName"],
            "foreign_keys": ["Departments", "Managers"],
        },
    },
    {
        "query": """
        CREATE TABLE "ComplexOrders" (
            "OrderId" UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            "CustomerName" VARCHAR(200) NOT NULL,
            "OrderDate" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            "TotalAmount" DECIMAL(15,2) NOT NULL CHECK ("TotalAmount" > 0),
            "Status" VARCHAR(50) DEFAULT 'Pending' CHECK ("Status" IN ('Pending', 'Processing', 'Completed', 'Cancelled')),
            "LastModified" TIMESTAMP,
            "Version" INT DEFAULT 1,
            "ProcessedBy" VARCHAR(100),
            "Priority" INT GENERATED ALWAYS AS (
                CASE 
                    WHEN "TotalAmount" > 1000 THEN 1
                    WHEN "TotalAmount" > 500 THEN 2
                    ELSE 3
                END
            ) STORED,
            "SearchVector" tsvector GENERATED ALWAYS AS (
                setweight(to_tsvector('english', COALESCE("CustomerName", '')), 'A')
            ) STORED,
            CONSTRAINT "unique_order" UNIQUE ("OrderId", "Version"),
            CONSTRAINT "valid_dates" CHECK ("LastModified" >= "OrderDate"),
            FOREIGN KEY ("ProcessedBy") REFERENCES "Employees"("EmployeeId") ON DELETE SET NULL,
            FOREIGN KEY ("CustomerId") REFERENCES "Customers"("Id") ON UPDATE CASCADE,
            FOREIGN KEY ("RegionId") REFERENCES "Regions"("Id") ON DELETE RESTRICT
        )
        PARTITION BY RANGE ("OrderDate");
        """,
        "expected_output": {
            "table_name": "ComplexOrders",
            "columns": [
                "OrderId",
                "CustomerName",
                "OrderDate",
                "TotalAmount",
                "Status",
                "LastModified",
                "Version",
                "ProcessedBy",
                "Priority",
                "SearchVector",
            ],
            "foreign_keys": ["Employees", "Customers", "Regions"],
        },
    },
    {
        "query": """
        CREATE TABLE "artists" (
            "artist_id" SERIAL PRIMARY KEY,
            "artist_name" VARCHAR NOT NULL CHECK (artist_name <> ''),
            "artist_count" INTEGER CHECK (artist_count >= 0)
        );
        """,
        "expected_output": {
            "table_name": "artists",
            "columns": ["artist_id", "artist_name", "artist_count"],
            "foreign_keys": [],
        },
    },
    # {
    #     "query": """
    #     CREATE TABLE "tracks" (
    #         "track_id" SERIAL PRIMARY KEY,
    #         "track_name" VARCHAR NOT NULL CHECK (track_name <> ''),
    #         "artist_id" INTEGER NOT NULL REFERENCES "artists"("artist_id") ON DELETE CASCADE,
    #         "released_year" INTEGER CHECK (released_year >= 1900 AND released_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    #         "released_month" INTEGER CHECK (released_month >= 1 AND released_month <= 12),
    #         "released_day" INTEGER CHECK (released_day >= 1 AND released_day <= 31),
    #         "bpm" INTEGER CHECK (bpm > 0),
    #         "key" VARCHAR CHECK (key IN ('C Major', 'C Minor', 'C# Major', 'C# Minor', 'D Major', 'D Minor', 'D# Major', 'D# Minor', 'E Major', 'E Minor', 'F Major', 'F Minor', 'F# Major', 'F# Minor', 'G Major', 'G Minor', 'G# Major', 'G# Minor', 'A Major', 'A Minor', 'A# Major', 'A# Minor', 'B Major', 'B Minor')),
    #         "mode" VARCHAR CHECK (mode IN ('Major', 'Minor')),
    #         "danceability_%" INTEGER CHECK (danceability_% >= 0 AND danceability_% <= 100),
    #         "valence_%" INTEGER CHECK (valence_% >= 0 AND valence_% <= 100),
    #         "energy_%" INTEGER CHECK (energy_% >= 0 AND energy_% <= 100),
    #         "acousticness_%" INTEGER CHECK (acousticness_% >= 0 AND acousticness_% <= 100),
    #         "instrumentalness_%" INTEGER CHECK (instrumentalness_% >= 0 AND instrumentalness_% <= 100),
    #         "liveness_%" INTEGER CHECK (liveness_% >= 0 AND liveness_% <= 100),
    #         "speechiness_%" INTEGER CHECK (speechiness_% >= 0 AND speechiness_% <= 100),
    #         "cover_url" VARCHAR CHECK (cover_url ~* '^https?://')
    #     );
    #     """,
    #     "expected_output": {
    #         "table_name": "tracks",
    #         "columns": [
    #             "track_id",
    #             "track_name",
    #             "artist_id",
    #             "released_year",
    #             "released_month",
    #             "released_day",
    #             "bpm",
    #             "key",
    #             "mode",
    #             "danceability_%",
    #             "valence_%",
    #             "energy_%",
    #             "acousticness_%",
    #             "instrumentalness_%",
    #             "liveness_%",
    #             "speechiness_%",
    #             "cover_url",
    #         ],
    #         "foreign_keys": ["artists"],
    #     },
    # },
    {
        "query": """
        CREATE TABLE "streaming_data" (
            "streaming_id" SERIAL PRIMARY KEY,
            "track_id" INTEGER NOT NULL REFERENCES "tracks"("track_id") ON DELETE CASCADE,
            "in_spotify_playlists" INTEGER CHECK (in_spotify_playlists >= 0),
            "in_spotify_charts" INTEGER CHECK (in_spotify_charts >= 0),
            "streams" BIGINT CHECK (streams >= 0),
            "in_apple_playlists" INTEGER CHECK (in_apple_playlists >= 0),
            "in_apple_charts" INTEGER CHECK (in_apple_charts >= 0),
            "in_deezer_playlists" INTEGER CHECK (in_deezer_playlists >= 0),
            "in_deezer_charts" INTEGER CHECK (in_deezer_charts >= 0),
            "in_shazam_charts" INTEGER CHECK (in_shazam_charts >= 0)
        );
        """,
        "expected_output": {
            "table_name": "streaming_data",
            "columns": [
                "streaming_id",
                "track_id",
                "in_spotify_playlists",
                "in_spotify_charts",
                "streams",
                "in_apple_playlists",
                "in_apple_charts",
                "in_deezer_playlists",
                "in_deezer_charts",
                "in_shazam_charts",
            ],
            "foreign_keys": ["tracks"],
        },
    },
]


@pytest.mark.parametrize("case", test_cases)
def test_sql_parser(case):
    output = parse_sql_query(case["query"])
    expected_output = case["expected_output"]
    assert (
        output["name"] == expected_output["table_name"]
    ), f"Table name mismatch: {output['name']} != {expected_output['table_name']}"
    assert set(output["columns"]) == set(
        expected_output["columns"]
    ), f"Column mismatch: {output['columns']} != {expected_output['columns']}"
    assert set(output["foreign_keys"]) == set(
        expected_output["foreign_keys"]
    ), f"Foreign keys mismatch: {output['foreign_keys']} != {expected_output['foreign_keys']}"


helper_test_cases = [
    {
        "query": """
        CREATE TABLE "TestTable" (
            "Id" INT PRIMARY KEY,
            "Name" VARCHAR(100) NOT NULL,
            "Email" VARCHAR(200) UNIQUE,
            "DepartmentId" INT,
            FOREIGN KEY ("DepartmentId") REFERENCES "Departments"("Id"),
            CONSTRAINT "age_check" CHECK ("Age" >= 18),
            CONSTRAINT "unique_email_name" UNIQUE ("Email", "Name")
        );
        """,
        "expected": {
            "column_names": ["Id", "Name", "Email", "DepartmentId"],
            "foreign_keys": ["Departments"],
        },
    },
    {
        "query": """
        CREATE TABLE "Orders" (
            "OrderId" INT,
            "CustomerId" INT,
            "ProductId" INT,
            FOREIGN KEY ("CustomerId") REFERENCES "Customers"("Id"),
            FOREIGN KEY ("ProductId") REFERENCES "Products"("Id"),
            CONSTRAINT "valid_order" CHECK ("OrderId" > 0)
        );
        """,
        "expected": {
            "column_names": ["OrderId", "CustomerId", "ProductId"],
            "foreign_keys": ["Customers", "Products"],
        },
    },
]


@pytest.mark.parametrize("case", helper_test_cases)
def test_extract_column_names(case):
    parsed = parse_sql(case["query"])
    columns = extract_column_names_from_parsed_query(parsed)
    assert set(columns) == set(case["expected"]["column_names"])


@pytest.mark.parametrize("case", helper_test_cases)
def test_extract_foreign_keys(case):
    parsed = parse_sql(case["query"])
    foreign_keys = extract_foreign_keys_from_parsed_query(parsed)
    assert set(foreign_keys) == set(case["expected"]["foreign_keys"])


@pytest.mark.parametrize("case", helper_test_cases)
def test_extract_columns_from_parsed_query(case):
    parsed = parse_sql(case["query"])
    columns = extract_columns_from_parsed_query(parsed)
    # TODO: Add more tests
    assert len(columns) == len(case["expected"]["column_names"])


@pytest.mark.parametrize("case", helper_test_cases)
def test_extract_table_constraints(case):
    parsed = parse_sql(case["query"])
    constraints = extract_table_constraints_from_parsed_query(parsed)
    # TODO: Add more tests
    assert isinstance(constraints, list)
