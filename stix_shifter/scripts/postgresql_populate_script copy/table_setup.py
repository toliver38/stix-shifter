import json
import csv
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

class TableSetup():

    def __init__(self, connection_params, database, table, data_file):
        connection_params = json.loads(connection_params)
        self.user = connection_params["user"]
        self.password = connection_params["password"]
        self.host = connection_params["host"]
        self.port = connection_params.get("port", 5432)
        self.database = database
        self.table = table
        self.csv_reader = csv.reader(data_file, delimiter=',')
        self.csv_rows = []
        self.fields_list = []
        self.data_types_list = []
        self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()

    def create_database(self):
        print(f"Creating database: {self.database}")
        try:
            engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/postgres')
            conn = engine.connect()
            conn.execute(f"DROP DATABASE IF EXISTS {self.database}")
            conn.execute(f"CREATE DATABASE {self.database}")
            conn.close()
            print(f"{self.database} created")
        except SQLAlchemyError as err:
            print(f"Error: {err}")

    def drop_table(self):
        print(f"Dropping table: {self.table}")
        try:
            self.metadata.reflect(bind=self.engine)
            table = Table(self.table, self.metadata, autoload_with=self.engine)
            table.drop(self.engine)
            print(f"{self.table} dropped")
        except SQLAlchemyError as err:
            print(f"Error: {err}")

    def create_table(self):
        try:
            for row in self.csv_reader:
                self.csv_rows.append(row)

            self.fields_list = self.csv_rows[0]
            self.data_types_list = self.csv_rows[1]

            columns = []
            for index, field in enumerate(self.fields_list):
                data_type = self.data_types_list[index]
                if data_type.lower() == 'int':
                    columns.append(Column(field, Integer))
                elif data_type.lower() == 'varchar':
                    columns.append(Column(field, String))
                else:
                    raise ValueError(f"Unsupported data type: {data_type}")

            table = Table(self.table, self.metadata, *columns)
            table.create(self.engine)
            print(f"Created {self.table} with fields: {self.fields_list}")
        except SQLAlchemyError as err:
            print(f"Error: {err}")

    def populate_table(self):
        print(f"Populating {self.table} with data")
        try:
            self.metadata.reflect(bind=self.engine)
            table = Table(self.table, self.metadata, autoload_with=self.engine)
            conn = self.engine.connect()
            for index, row in enumerate(self.csv_rows):
                if index < 2:
                    continue
                ins = table.insert().values({field: value for field, value in zip(self.fields_list, row)})
                conn.execute(ins)
            conn.close()
            print(f"{self.table} populated with data")
        except SQLAlchemyError as err:
            print(f"Error: {err}")