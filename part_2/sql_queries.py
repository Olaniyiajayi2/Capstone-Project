import configparser

# configurations and credentials
config = configparser.ConfigParser()
config.read('conf1.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
COVID_DATA = config['S3']['INPUT_DATA']

#DROP TABLES IF EXIST

main_covid_table_drop = "DROP TABLE IF EXISTS main_covid_table"
us_covid_table_drop = "DROP TABLE IF EXISTS us_covid_table"
china_covid_table_drop = "DROP TABLE IF EXISTS china_covid_table"
india_covid_table_drop = "DROP TABLE IF EXISTS india_covid_table"
russia_covid_table_drop = "DROP TABLE IF EXISTS russia_covid_table"

# CREATE TABLES
main_covid_table_create = ("""
                            CREATE TABLE IF NOT EXISTS main_covid_table(
                            SNo INT IDENTITY(1, 1),
                            ObservationDate DATETIME,
                            state TEXT,
                            country TEXT,
                            lastUpdate DATE,
                            Confirmed DOUBLE PRECISION,
                            Deaths DOUBLE PRECISION,
                            Recovered DOUBLE PRECISION
                            )
                            """)


us_covid_table_create = ("""
                            CREATE TABLE IF NOT EXISTS us_covid_table(
                            ObservationDate DATETIME,
                            state TEXT,
                            country TEXT,
                            lastUpdate DATE,
                            Confirmed DOUBLE PRECISION,
                            Deaths DOUBLE PRECISION,
                            Recovered DOUBLE PRECISION
                            )
                            """)


china_covid_table_create = ("""
                            CREATE TABLE IF NOT EXISTS china_covid_table(
                            ObservationDate DATETIME,
                            state TEXT,
                            country TEXT,
                            lastUpdate DATE,
                            Confirmed DOUBLE PRECISION,
                            Deaths DOUBLE PRECISION,
                            Recovered DOUBLE PRECISION
                            )
                            """)

india_covid_table_create = ("""
                            CREATE TABLE IF NOT EXISTS india_covid_table(
                            ObservationDate DATETIME,
                            state TEXT,
                            country TEXT,
                            lastUpdate DATE,
                            Confirmed DOUBLE PRECISION,
                            Deaths DOUBLE PRECISION,
                            Recovered DOUBLE PRECISION
                            )
                            """)

russia_covid_table_create = ("""
                            CREATE TABLE IF NOT EXISTS russia_covid_table(
                            ObservationDate DATETIME,
                            state TEXT,
                            country TEXT,
                            lastUpdate DATE,
                            Confirmed DOUBLE PRECISION,
                            Deaths DOUBLE PRECISION,
                            Recovered DOUBLE PRECISION
                            )
                            """)


# STAGING TABLES
staging_main_covid_table_copy = ("""
                                COPY main_covid_table
                                FROM {}
                                iam_role {}
                                json 'auto ignorecase'
                                DATEFORMAT 'YYYY-MM-DD'
                                IGNOREHEADER 1
                                """).format(COVID_DATA, IAM_ROLE)


# FINAL INSERT TABLES
us_table_copy = ("""
                INSERT INTO us_covid_table(
                                            ObservationDate,
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            )
                                            
                                            SELECT 
                                            distinct ObservationDate, 
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            FROM main_covid_table
                                            WHERE country = 'US'
                """)

china_table_copy = ("""
                INSERT INTO china_covid_table(
                                            ObservationDate,
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            )
                                            
                                            SELECT 
                                            distinct ObservationDate, 
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            FROM main_covid_table
                                            WHERE country = 'Mainland China'
                """)


india_table_copy = ("""
                INSERT INTO india_covid_table(
                                            ObservationDate,
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            )
                                            
                                            SELECT 
                                            distinct ObservationDate, 
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            FROM main_covid_table
                                            WHERE country = 'India'
                """)


russia_table_copy = ("""
                INSERT INTO russia_covid_table(
                                            ObservationDate,
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            )
                                            
                                            SELECT 
                                            distinct ObservationDate, 
                                            state,
                                            country,
                                            lastUpdate,
                                            Confirmed,
                                            Deaths,
                                            Recovered
                                            FROM main_covid_table
                                            WHERE country = 'Russia'
                """)


create_table_queries = [main_covid_table_create, us_covid_table_create, china_covid_table_create, india_covid_table_create, russia_covid_table_create]
drop_table_queries = [main_covid_table_drop, us_covid_table_drop, china_covid_table_drop, india_covid_table_drop, russia_covid_table_drop]
copy_table_queries = [staging_main_covid_table_copy]
insert_table_queries = [us_table_copy, china_table_copy, india_table_copy, russia_table_copy]