from psycopg2 import connect, Error, extras


class PgDatabase:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
        except Error as e:
            print("Error: Unable to connect to the database.")
            print(e)

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None

    def create_table(self):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS pc_metadata_2201m (
                id SERIAL PRIMARY KEY,
                version DOUBLE PRECISION,
                source_file TEXT,
                point_count INT,
                head_length INT,
                tail_length INT,
                scales DOUBLE PRECISION[],
                offsets DOUBLE PRECISION[],
                bbox DOUBLE PRECISION[]
            );
            CREATE TABLE IF NOT EXISTS pc_record_2201m (
                meta_id INT,
                sfc_head INT,
                sfc_tail INT[],
                z DOUBLE PRECISION[], 
                PRIMARY KEY (meta_id, sfc_head)
            );
            """
        # scales DOUBLE PRECISION[],
        # offsets DOUBLE PRECISION[],
        # meta_id INT REFERENCE pc_metadata(id),
        # sfc_head INT,
        # PRIMARY KEY (meta_id, sfc_head)
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
        except Error as e:
            print("Error: Unable to create table")
            print(e)
            self.connection.rollback()

    def execute_query(self, sql, data=None):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        try:
            if data:
                self.cursor.execute(sql, data)
            else:
                self.cursor.execute(sql)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to execute query: {sql}")
            print(e)
            self.connection.rollback()

    def execute_copy(self, filename):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        # Use the COPY command to insert point records into the table
        with open(filename, 'r') as f:
            try:
                self.cursor.copy_expert(sql="COPY pc_record_2201m FROM stdin WITH CSV HEADER", file=f)
                self.connection.commit()
                print("Data copied successfully.")
            except Error as e:
                print("Error: Unable to copy the data.")
                print(e)
                self.connection.rollback()
