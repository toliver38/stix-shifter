import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class APIClient():
    
    def __init__(self, connection, configuration):
        auth = configuration.get('auth')
        self.user = auth.get('username')
        self.password = auth.get('password')
        self.timeout = connection['options'].get('timeout')
        self.result_limit = connection['options'].get('result_limit')
        self.host = connection.get("host")
        self.database = connection.get("database")
        self.table = connection['options'].get("table")
        self.port = connection.get("port")
        
        self.engine = create_async_engine(
            f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            echo=True
        )
        self.Session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession
        )

    async def ping_data_source(self):
        # Pings the data source
        response = {"code": 200, "message": "All Good!"}
        try:
            async with self.Session() as session:
                async with session.begin():
                    result = await session.execute("SELECT 42;")
                    (r,) = result.fetchone()
                    assert r == 42
        except SQLAlchemyError as err:
            response["code"] = err.code
            response["message"] = str(err)
        except Exception as err:
            response["code"] = 'unknown'
            response["message"] = str(err)

        return response

    async def run_search(self, query, start=0, rows=0):
        # Return the search results. Results must be in JSON format before translating into STIX 
        response = {"code": 200, "message": "All Good!", "result": []}

        try:
            async with self.Session() as session:
                async with session.begin():
                    column_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table}';"
                    result = await session.execute(column_query)
                    column_collection = result.fetchall()
                    column_list = [row[0] for row in column_collection]

                    result = await session.execute(query)
                    result_collection = result.fetchall()

            results_list = []
            row_count = int(rows)

            # Put table data in JSON format
            for index, tuple in enumerate(result_collection):
                if index < int(start):
                    continue
                if row_count < 1:
                    break
                results_object = {}
                for index, datum in enumerate(tuple):
                    results_object[column_list[index]] = datum
                results_list.append(results_object)
                row_count -= 1

                response["result"] = results_list
        except SQLAlchemyError as err:
            response["code"] = err.code
            response["message"] = str(err)
        except Exception as err:
            response["code"] = 'unknown'
            response["message"] = str(err)

        return response
