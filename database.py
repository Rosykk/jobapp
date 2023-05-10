from configparser import ConfigParser
import psycopg2


class Database:
    def __init__(self, filename='config/database.ini', section='postgresql'):
        self.conn = None
        self.filename = filename
        self.section = section

    def connect(self):
        """Create a connection to the postgresql database."""
        try:
            params = self.load_config()
            print('[Database] trying to connect to database')
            self.conn = psycopg2.connect(**params)

            cursor = self.conn.cursor()
            cursor.execute('SELECT version()')
            version = cursor.fetchone()
            cursor.close()
            print(f'[Database] version: {version}')
        except (Exception, psycopg2.DatabaseError) as err:
            print(err)

    def disconnect(self):
        """Close the connection to the postgresql database."""
        if self.conn is not None:
            try:
                self.conn.close()
                print('[Database] connection closed.')
            except psycopg2.Error as e:
                print('[Database] error while closing connection:', e)
            finally:
                self.conn = None

    def load_config(self):
        """Load the configuration for the postgresql database."""
        cparser = ConfigParser()
        cparser.read(self.filename)

        database = {}

        # check if config has postgresql section
        if cparser.has_section(self.section):
            params = cparser.items(self.section)
            for p in params:
                database[p[0]] = p[1]
        else:
            raise Exception('[Config] section not found!')

        return database

    def execute_query(self, query, params=None):
        """Execute a sql query on the postgresql database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchone()
            cursor.close()

            return results
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

            return None

    def commit(self):
        """Commit changes to the postgresql database."""
        try:
            self.conn.commit()
        except psycopg2.Error as e:
            print('[Database] error while committing changes:', e)
