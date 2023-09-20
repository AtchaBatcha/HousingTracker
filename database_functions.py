"""Contains functions primarily used for database querying"""
import sqlalchemy
import pandas as pd
class SqlOperations():
    def __init__(self, config):
        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['ip']}/{config['database']}")


        self.engine = engine
        self.table_name = config['table']

    def retrieve_all_current_db_records(self) -> pd.DataFrame:
        """Retrieves all records from the rightmove database, if duplicate house_id's are present,
        take the most recent one based on date added to db
        """


        query = f"""SELECT T.*
                    FROM (
                         SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY house_id ORDER BY dateaddedtodb DESC) AS ROWNumber
                        FROM {self.table_name}
                         ) AS T
                    WHERE ROWNumber = 1
                    AND T.datetakenoffwebsite IS NULL;"""

        with self.engine.connect() as connection:
            df = pd.read_sql(query, connection, index_col='ID')

        df['students'] = df['students'].astype('bool')
        df['auction'] = df['auction'].astype('bool')

        df.drop(columns='ROWNumber', inplace=True)

        return df

    def insert_dataframe(self, df: pd.DataFrame):
        with self.engine.connect() as connection:
            df.to_sql(self.table_name, connection, if_exists='append', index=False)

    def test_connection(self):

        try:
            connection = self.engine.connect()
            print('connection successful')
            connection.close()
        except:
            print('connection failed')

