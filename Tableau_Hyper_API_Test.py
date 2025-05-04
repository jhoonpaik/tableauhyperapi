# -*- coding: utf-8 -*-
"""

Tableau Hyper API CUID Test
*   Create
*   Update
*   Insert
*   Delete

"""

import pandas
import numpy
import shutil

from datetime import datetime
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException

# Table Definition
orders_table = TableDefinition(
    # public namespace
    table_name="Orders",
    columns=[
        TableDefinition.Column(name="Address ID", type=SqlType.small_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Customer ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order Date", type=SqlType.date(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Ship Date", type=SqlType.date(), nullability=NULLABLE),
        TableDefinition.Column(name="Ship Mode", type=SqlType.text(), nullability=NULLABLE)
    ]
)

customer_table = TableDefinition(
    # public namespace
    table_name="Customer",
    columns=[
        TableDefinition.Column(name="Customer ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Customer Name", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Loyalty Reward Points", type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Segment", type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)

products_table = TableDefinition(
    # public namespace
    table_name="Products",
    columns=[
        TableDefinition.Column(name="Category", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Product ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Product Name", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Sub-Category", type=SqlType.text(), nullability=NOT_NULLABLE)
    ]
)

line_items_table = TableDefinition(
    # public namespace
    table_name="Line Items",
    columns=[
        TableDefinition.Column(name="Line Item ID", type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Order ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Product ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Sales", type=SqlType.double(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Quantity", type=SqlType.small_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Discount", type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name="Profit", type=SqlType.double(), nullability=NOT_NULLABLE)
    ]
)

test_table = TableDefinition(
    # public namespace
    table_name="test Items",
    columns=[
        TableDefinition.Column(name="Test Item ID", type=SqlType.big_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Test ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Test Product ID", type=SqlType.text(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Test Sales", type=SqlType.double(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Test Quantity", type=SqlType.small_int(), nullability=NOT_NULLABLE),
        TableDefinition.Column(name="Test Discount", type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name="Test Profit", type=SqlType.double(), nullability=NOT_NULLABLE)
    ]
)

#create table
def run_create_data_into_multiple_tables():

    print("EXAMPLE - Create data into multiple tables within a new Hyper file")
    path_to_database = Path("superstore.hyper")

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            # Create multiple tables.
            connection.catalog.create_table(table_definition=orders_table)
            connection.catalog.create_table(table_definition=customer_table)
            connection.catalog.create_table(table_definition=products_table)
            connection.catalog.create_table(table_definition=line_items_table)
            connection.catalog.create_table(table_definition=test_table)

            tables = [test_table]
            for table in tables:
                # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
                row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {table.table_name}")
                print(f"The number of rows in table {table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")

#data insert
def run_insert_data_into_multiple_tables():
    """
    An example of how to create and insert data into a multi-table Hyper file where tables have different types
    """
    print("EXAMPLE - Insert data into multiple tables within a new Hyper file")
    path_to_database = Path("superstore.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "superstore.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            
            connection.catalog.create_table(table_definition=orders_table)
            connection.catalog.create_table(table_definition=customer_table)
            connection.catalog.create_table(table_definition=products_table)
            connection.catalog.create_table(table_definition=line_items_table)
            connection.catalog.create_table(table_definition=test_table)

            # Insert data into Orders table.
            orders_data_to_insert = [
                [399, "DK-13375", datetime(2012, 9, 7), "CA-2011-100006", datetime(2012, 9, 13), "Standard Class"],
                [530, "EB-13705", datetime(2012, 7, 8), "CA-2011-100090", datetime(2012, 7, 12), "Standard Class"],
                [777, "SM-24680", datetime(2013, 3, 2), "CA-2011-100099", datetime(2012, 3, 12), "Standard Class"]
            ]

            with Inserter(connection, orders_table) as inserter:
                inserter.add_rows(rows=orders_data_to_insert)
                inserter.execute()

            # Insert data into Customers table.
            customer_data_to_insert = [
                ["DK-13375", "Dennis Kane", 518, "Consumer"],
                ["EB-13705", "Ed Braxton", 815, "Corporate"],
                ["SM-24680", "Sad Mushroom", 816, "Corporate"]
            ]

            with Inserter(connection, customer_table) as inserter:
                inserter.add_rows(rows=customer_data_to_insert)
                inserter.execute()

            # Insert individual row into Product table.
            with Inserter(connection, products_table) as inserter:
                inserter.add_row(row=["TEC-PH-10002075", "Technology", "Phones", "AT&T EL51110 DECT"])
                inserter.execute()

            # Insert data into Line Items table.
            line_items_data_to_insert = [
                [2718, "CA-2011-100006", "TEC-PH-10002075", 377.97, 3, 0.0, 109.6113],
                [2719, "CA-2011-100090", "TEC-PH-10002075", 377.97, 3, None, 109.6113]
            ]

            with Inserter(connection, line_items_table) as inserter:
                inserter.add_rows(rows=line_items_data_to_insert)
                inserter.execute()

            tables = [orders_table, customer_table, products_table, line_items_table, test_table]
            for table in tables:
                # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
                row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {table.table_name}")
                print(f"The number of rows in table {table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")

#data update
def run_update_data_into_multiple_tables():
    """
    An example of how to update data into a multi-table Hyper file where tables have different types
    """
    print("EXAMPLE_2 - Update data into multiple tables within a new Hyper file")
    path_to_source_database = Path("superstore.hyper")

    path_to_database = Path(shutil.copy(path_to_source_database, "superstore_sample_update.hyper")).resolve()

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            rows_pre_update = connection.execute_list_query(
                query=f"SELECT {escape_name('Order Date')}, {escape_name('Order ID')}"
                f"FROM {escape_name('Orders')}")
            print(f"Pre-Update: Individual rows showing 'Order Date' and 'Order ID' "
                  f"columns: {rows_pre_update}\n")

            print("Update 'Orders' table by July 2012 date.")
            row_count = connection.execute_command(
                command=f"UPDATE {escape_name('Orders')} "
                f"SET {escape_name('Order Date')} = {escape_name('Order Date')} + 10 "
                f"WHERE {escape_name('Order Date')} <= '2012-08-01' OR {escape_name('Order Date')} >= '2013-03-01'")

            print(f"The number of updated rows in table {escape_name('Orders')} is {row_count}")

            rows_post_update = connection.execute_list_query(
                query=f"SELECT {escape_name('Order Date')}, {escape_name('Order ID')} "
                f"FROM {escape_name('Orders')}")
            print(f"Post-Update: Individual rows showing 'Order Date' and 'Order ID'"
                  f"columns: {rows_post_update}")

            tables = [orders_table, customer_table, products_table, line_items_table]
            for table in tables:
                # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
                row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {table.table_name}")
                print(f"The number of rows in table {table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")

#date delete
def run_delete_data_in_existing_hyper_file():
    """
    An example of how to delete data in an existing Hyper file.
    """
    print("EXAMPLE_3 - Delete data from an existing Hyper file")

    #path_to_source_database = Path(__file__).parent / "data" / "superstore_sample.hyper"
    path_to_source_database = "superstore.hyper"

    # Make a copy of the superstore example Hyper file.
    path_to_database = Path(shutil.copy(path_to_source_database, "superstore_sample_delete.hyper")).resolve()

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file "superstore_sample_delete.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            print(f"Delete all rows from customer with the name 'ED' from table {escape_name('Customer')}")

            row_count = connection.execute_command(
                command=f"DELETE FROM {escape_name('Customer')} "
                f"WHERE {escape_name('Customer ID')} = ANY("
                f"SELECT {escape_name('Customer ID')} FROM {escape_name('Orders')} "
                f"WHERE {escape_name('Order Date')} <= '2012-08-01')")
            
            print(f"The number of deleted rows in table {escape_name('Customer')} "
                  f"is {row_count}.\n")

            print(f"Delete all rows from customer with the name 'Ed Braxton' from table {escape_name('Orders')}")
            row_count = connection.execute_command(
                command=f"DELETE FROM {escape_name('Orders')} "
                f"WHERE {escape_name('Order Date')} <= '2012-08-01'")

            print(f"The number of deleted rows in table Customer is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")

# Function Main Test
if __name__ == '__main__':
    try:
        run_insert_data_into_multiple_tables()
        run_update_data_into_multiple_tables()
        run_delete_data_in_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)