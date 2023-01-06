# Packages
import psycopg2
import psycopg2.extras as extras
import config_warehouse as creds
import config_lake as creds_lake
import pandas as pd
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)  # allow int64 for db import


# Function to insert dataframe into database
def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cur = conn.cursor()
    try:
        extras.execute_values(cur, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1
    print("the dataframe is inserted")
    cur.close()


# Dict Grossregionen
regions_dict = {'Lake Geneva region': {'GE': 'Geneva', 'VD': 'Vaud', 'VS': 'Valais'}, \
                'Espace Mitteland': {'BE': 'Bern', 'SO': 'Solothurn', 'FR': 'Fribourg',
                                     'NE': 'Neuchatel', 'JU': 'Jura'}, \
                'Eastern Switzerland': {'SG': 'St. Gallen', 'TG': 'Thurgau',
                                        'AI': 'Appenzell Innerrhoden', 'AR': 'Appenzell Ausserrhoden',
                                        'GL': 'Glarus', 'SH': 'Schaffhausen', 'GR': 'Graubünden'}, \
                'Zurich': {'ZH': 'Zurich'}, \
                'Central Switzerland': {'UR': 'Uri', 'SZ': 'Schwyz', 'OW': 'Obwalden',
                                        'NW': 'Nidwalden', 'LU': 'Lucerne', 'ZG': 'Zug'}, \
                'Northwestern Switzerland': {'BS': 'Basel-Stadt', 'BL': 'Basel-Landschaft',
                                             'AG': 'Aargau'}, \
                'Ticino': {'TI': 'Ticino'}}

# Tables Lake with warehouse names
tables_lake = ['measuring_points_coordinates', 'postal_codes', \
               'charging_stations_occupancy', 'electric_cars_21', 'electricity_production_plants', \
               'measuring_points', 'charging_stations_static', 'charging_stations_locations']


# Create Table Grossregionen / Kantone
def create_table_regions():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Regions_and_cantons (
            Canton_abbreviation CHAR(2) PRIMARY KEY,
            Canton_name VARCHAR(255) NOT NULL,
            Region_name VARCHAR(255) NOT NULL
            )
        """,
        """ INSERT INTO Regions_and_cantons(Canton_abbreviation, Canton_name, Region_name)
            VALUES
            ('GE', 'Geneva', 'Lake Geneva region'),
            ('VD', 'Vaud', 'Lake Geneva region'),
            ('VS', 'Valais', 'Lake Geneva region'),

            ('BE','Bern', 'Espace Mitteland'),
            ('SO', 'Solothurn', 'Espace Mitteland'),
            ('FR', 'Fribourg', 'Espace Mitteland'),
            ('NE', 'Neuchatel', 'Espace Mitteland'),
            ('JU', 'Jura', 'Espace Mitteland'),

            ('SG', 'St. Gallen', 'Eastern Switzerland'),
            ('TG', 'Thurgau', 'Eastern Switzerland'),
            ('AI', 'Appenzell Innerrhoden', 'Eastern Switzerland'),
            ('AR', 'Appenzell Ausserrhoden', 'Eastern Switzerland'),
            ('GL', 'Glarus', 'Eastern Switzerland'),
            ('SH', 'Schaffhausen', 'Eastern Switzerland'),
            ('GR', 'Graubünden', 'Eastern Switzerland'),

            ('UR', 'Uri', 'Central Switzerland'),
            ('SZ', 'Schwyz', 'Central Switzerland'),
            ('OW', 'Obwalden', 'Central Switzerland'),
            ('NW', 'Nidwalden', 'Central Switzerland'),
            ('LU', 'Lucerne', 'Central Switzerland'),
            ('ZG', 'Zug', 'Central Switzerland'),

            ('BS', 'Basel-Stadt', 'Northwestern Switzerland'),
            ('BL', 'Basel-Landschaft', 'Northwestern Switzerland'),
            ('AG', 'Aargau', 'Northwestern Switzerland'),

            ('ZH', 'Zurich', 'Zurich'),

            ('TI', 'Ticino', 'Ticino')
        """
    )
    try:
        # Set up a connection to the postgres server.
        conn_string = "host=" + creds.PGHOST + " port=" + "5432" + " dbname=" + creds.PGDATABASE + " user=" + creds.PGUSER \
                      + " password=" + creds.PGPASSWORD

        conn = psycopg2.connect(conn_string)
        print("Connected!")

        # Create a cursor object
        cursor = conn.cursor()
        # Create tables
        for command in commands:
            cursor.execute(command)
        print('Tables created')
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Fetch Table plz
def fetch_data_plz():
    try:
        # DB connection
        conn_string = "host=" + creds_lake.PGHOST + " port=" + "5432" + " dbname=" + creds_lake.PGDATABASE + " user=" + creds_lake.PGUSER + " password=" + creds_lake.PGPASSWORD
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        # Select data
        cur.execute(
            """ SELECT DISTINCT postleitzahl, kanton FROM public."ZIPCode" where kanton not in ('FL', 'DE', 'IT')""")  # plz can occur twice with different cantons
        # cur.execute(""" SELECT DISTINCT postleitzahl, kanton FROM public."ZIPCode" where kanton not in ('FL', 'DE', 'IT') GROUP BY postleitzahl, kanton HAVING COUNT(postleitzahl) = 1;""")
        df_records = cur.fetchall()
        df = pd.DataFrame(df_records, columns=['postal_code', 'canton_abbreviation'])
        print(df.info())
        print(df[df.duplicated() == True])  # check for duplicates
        # df.drop_duplicates(subset=['postalcode'], keep='last', inplace=True)
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()


# Create Table PLZ
def create_table_plz():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Postal_codes (
            ID_postal_code SERIAL PRIMARY KEY,
            Postal_code INT NOT NULL,
            Canton_abbreviation CHAR(2) NOT NULL,
            FOREIGN KEY (Canton_abbreviation) REFERENCES regions_and_cantons(canton_abbreviation))
        """
    )
    try:
        # Set up a connection to the postgres server.
        conn_string = "host=" + creds.PGHOST + " port=" + "5432" + " dbname=" + creds.PGDATABASE + " user=" + creds.PGUSER \
                      + " password=" + creds.PGPASSWORD

        conn = psycopg2.connect(conn_string)
        print("Connected!")

        # Create a cursor object
        cursor = conn.cursor()

        # Create tables
        # for command in commands:
        cursor.execute(commands)
        print('Tables created')

        # Import data from dataframe
        execute_values(conn, fetch_data_plz(), 'Postal_codes')

        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Create all other tables
def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Electric_cars_21 (
            ID SERIAL PRIMARY KEY,
            Vehicle_group VARCHAR(255) NOT NULL,
            Year_of_first_registration VARCHAR(255) NOT NULL,
            Fuel VARCHAR(255) NOT NULL,
            Count_2021 Int NOT NULL,
            Canton_abbreviation VARCHAR(255) NOT NULL,
            FOREIGN KEY (Canton_abbreviation) REFERENCES regions_and_cantons(canton_abbreviation))
        """,
        """
        CREATE TABLE IF NOT EXISTS Measuring_points (
            Nr_measuring_point Int PRIMARY KEY,
            Point_name VARCHAR(255) NOT NULL,
            Status VARCHAR(255) NOT NULL,
            Type_point VARCHAR(255) NOT NULL,
            Road VARCHAR(255) NOT NULL,
            Canton_abbreviation CHAR(2) NOT NULL,
            FOREIGN KEY (Canton_abbreviation) REFERENCES regions_and_cantons(canton_abbreviation))
        """,
        """
        CREATE TABLE IF NOT EXISTS Traffic_measurement_21 (
            Nr Int PRIMARY KEY,
            Nr_measuring_point Int NOT NULL,
            Annual_average Int)
        """,  # , FOREIGN KEY (Nr_measuring_point) REFERENCES Measuring_points(Nr_measuring_point) # set fk in db
        """
        CREATE TABLE IF NOT EXISTS Measuring_points_coordinates (
            Nr Serial PRIMARY KEY,
            Nr_measuring_point Int NOT NULL,
            Coordinate_east Int NOT NULL,
            Coordinate_nord Int NOT NULL,
            FOREIGN KEY (Nr_measuring_point) REFERENCES Measuring_points(Nr_measuring_point))
        """,
        """
        CREATE TABLE IF NOT EXISTS Main_categories_electricity_production (
            ID_main_category Int PRIMARY KEY,
            Name_main_category VARCHAR(255) NOT NULL)
        """,
        """
        CREATE TABLE IF NOT EXISTS Sub_categories_electricity_production (
            ID_sub_category Int PRIMARY KEY,
            Name_sub_category VARCHAR(255) NOT NULL,
            ID_main_category Int NOT NULL,
            FOREIGN KEY (ID_main_category)
            REFERENCES Main_categories_electricity_production(ID_main_category))
        """,
        """
        CREATE TABLE IF NOT EXISTS Electricity_production_plants (
            xtf_id Int PRIMARY KEY,
            ID_postal_code Int NOT NULL,
            Municipality VARCHAR(255) NOT NULL,
            Canton_abbreviation CHAR(2) NOT NULL,
            ID_sub_category INT NOT NULL,
            Total_power FLOAT NOT NULL,
            Avg_monthly_production_kwh FLOAT,
            FOREIGN KEY (ID_postal_code) REFERENCES Postal_codes(ID_postal_code),
            FOREIGN KEY (Canton_abbreviation) REFERENCES regions_and_cantons(canton_abbreviation),
            FOREIGN KEY (ID_sub_category)
            REFERENCES Sub_categories_electricity_production(ID_sub_category))
         """)

    try:
        # Set up a connection to the postgres server.
        conn_string = "host=" + creds.PGHOST + " port=" + "5432" + " dbname=" + creds.PGDATABASE + " user=" + creds.PGUSER \
                      + " password=" + creds.PGPASSWORD

        conn = psycopg2.connect(conn_string)
        print("Connected!")

        # Create a cursor object
        cursor = conn.cursor()

        # Create tables
        for command in commands:
            cursor.execute(command)
        print('Tables created')

        # Import data from dataframe
        # execute_values(conn, fetch_data_plz(), 'Postal_codes')

        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# Fetch other tables data / main import function
def fetch_all_data():
    try:
        # DB connection
        conn_string = "host=" + creds_lake.PGHOST + " port=" + "5432" + " dbname=" + creds_lake.PGDATABASE + " user=" + creds_lake.PGUSER + " password=" + creds_lake.PGPASSWORD
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # Select table names
        cur.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")
        records = cur.fetchall()
        records_list = []
        for item in records:
            item = str(item)
            records_list.append(item[2:-3])
        print(records_list)

        # Select data
        for table_name in range(0, len(records_list)):
            if records_list[table_name] == 'ZIPCode' or records_list[table_name] == 'RealTimeStatus':
                continue
            else:
                cur.execute((""" SELECT * FROM {}""").format(
                    records_list[table_name]))  # passing string without quotation marks
                df_records = cur.fetchall()
                df = pd.DataFrame(df_records)
                # print(df)

                if records_list[table_name] == 'measuringpointscoordinates':
                    df_selected = df.copy()
                    # Name columns
                    df_selected.columns = ['Nr_measuring_point', 'Status_BGDI', 'Point_name',
                                           'Canton_abbreviation', 'Road', 'Coordinate_East', 'Coordinate_Nord',
                                           'Status', 'Type_Point', 'Number_of_Lanes']
                    # Filter needed rows
                    # df_selected = df_selected.loc[(df_selected['Status'] == 'in Betrieb')
                    df_selected = df_selected.loc[df_selected['Type_Point'] != 'nur Online']

                    # Dataframe Measuring_points_coordinates set up
                    df_coordinates = df_selected[['Nr_measuring_point', 'Coordinate_East',
                                                  'Coordinate_Nord', ]]

                    # Dataframe Measuring_points; Drop not needed columns
                    df_selected.drop(['Status_BGDI', 'Coordinate_East', 'Coordinate_Nord', 'Number_of_Lanes'], axis=1,
                                     inplace=True)

                    # import to warehouse
                    execute_values(conn, df_selected, 'Measuring_points')
                    execute_values(conn, df_coordinates, 'Measuring_points_coordinates')
                    # print(df_selected)

                elif records_list[table_name] == 'realtimestatus':
                    continue

                elif records_list[table_name] == 'electriccars21':
                    # Name columns
                    df.columns = ['id', 'Canton_abbreviation', 'Vehicle_group',
                                  'Year_of_first_registration', 'Fuel', 'Count_2021']
                    # Filter needed rows
                    df_selected = df[(df['Canton_abbreviation'] != 'Switzerland') &
                                     (df['Canton_abbreviation'] != 'Confederation')]

                    # Map Canton names with Canton Abbreviations
                    dict_cantons = {'Zürich': 'ZH', 'Bern': 'BE', 'Luzern': 'LU', 'Uri': 'UR',
                                    'Schwyz': 'SZ', 'Obwalden': 'OW', 'Nidwalden': 'NW',
                                    'Glarus': 'GL', 'Zug': 'ZG', 'Fribourg': 'FR',
                                    'Solothurn': 'SO', 'Basel-Stadt': 'BS', 'Basel-Landschaft': 'BL',
                                    'Schaffhausen': 'SH', 'Appenzell-Ausserrhoden': 'AR',
                                    'Appenzell-Innerrhoden': 'AI', 'Sankt Gallen': 'SG',
                                    'Graubünden': 'GR', 'Aargau': 'AG', 'Thurgau': 'TG',
                                    'Ticino': 'TI', 'Vaud': 'VD', 'Valais': 'VS', 'Neuchâtel': 'NE',
                                    'Genève': 'GE', 'Jura': 'JU'}

                    df_selected['Canton_abbreviation'] = df_selected['Canton_abbreviation'].map(dict_cantons)
                    df_selected['Canton_abbreviation'] = df_selected['Canton_abbreviation'].astype(str)
                    # print(df_selected['Canton_abbreviation'])
                    # print(df_selected.info())

                    # import to warehouse
                    execute_values(conn, df_selected, 'Electric_cars_21')

                elif records_list[table_name] == 'electricityproductionplants':
                    # DFs main/sub categories
                    main_cat_dict = {'ID_main_category': [1, 2, 3, 4], 'Name_main_category': ['Fossil fuel',
                                                                                              'Hydroelectric power',
                                                                                              'Nuclear energy',
                                                                                              'Other renewable energies']}
                    df_selected_maincat = pd.DataFrame.from_dict(main_cat_dict)
                    sub_cat_dict = {'ID_sub_category': [1, 2, 3, 4, 5, 6, 7, 8], 'Name_sub_category': ['Biomass',
                                                                                                       'Crude oil',
                                                                                                       'Hydroelectric power',
                                                                                                       'Natural gas',
                                                                                                       'Nuclear energy',
                                                                                                       'Photovoltaic',
                                                                                                       'Waste',
                                                                                                       'Wind energy'],
                                    'ID_main_category': [4, 1, 2, 1, 3, 4, 4, 4]}
                    df_selected_subcat = pd.DataFrame.from_dict(sub_cat_dict)

                    # DF Electricity production plants
                    # Rename column names / select needed columns

                    df.columns = ['xtf_id', 'id_postal_code', 'Municipality', 'Canton_abbreviation', 'BoO', 'MC',
                                  'ID_sub_category', 'IP', 'total_power']

                    df_selected = df[['xtf_id', 'id_postal_code', 'Municipality', 'Canton_abbreviation',
                                      'ID_sub_category', 'total_power']]

                    # Map subcategories
                    dict_subcat = {'Biomass': 1, 'Crude oil': 2,
                                   'Hydroelectric power': 3,
                                   'Natural gas': 4,
                                   'Nuclear energy': 5,
                                   'Photovoltaic': 6,
                                   'Waste': 7,
                                   'Wind energy': 8}
                    df_selected['ID_sub_category'] = df_selected['ID_sub_category'].map(dict_subcat)

                    # Map postal code
                    try:

                        # Set up a connection to the warehouse
                        conn_string = "host=" + creds.PGHOST + " port=" + "5432" + " dbname=" + creds.PGDATABASE + " user=" + creds.PGUSER \
                                      + " password=" + creds.PGPASSWORD
                        conn = psycopg2.connect(conn_string)
                        print("Warehouse connected!")

                        # Create a cursor object
                        cursor = conn.cursor()

                        # # Catch Postal Codes
                        cursor.execute((""" SELECT * FROM Postal_codes"""))
                        df_records = cursor.fetchall()
                        df_postal_codes = pd.DataFrame(df_records, dtype='int')

                        # Append missing postal codes to relation postal_codes in warehouse
                        missing_plz_df = df[['id_postal_code', 'Canton_abbreviation']][df['id_postal_code']
                                                                                           .isin(
                            df_postal_codes[1]) == False]
                        # correct column names
                        missing_plz_df.rename(columns={'id_postal_code': 'postal_code'}, inplace=True)
                        print(missing_plz_df)
                        # Add primary key column
                        missing_plz_df['id_postal_code'] = pd.Series(dtype='int8')
                        missing_plz_df['id_postal_code'] = [id for id in range(3488, (3488 + len(missing_plz_df)))]
                        print(missing_plz_df)
                        execute_values(conn, missing_plz_df, 'Postal_codes')

                        # Catch Postal Codes AGAIN due to amendment
                        cursor.execute((""" SELECT * FROM Postal_codes"""))
                        df_records = cursor.fetchall()
                        df_postal_codes = pd.DataFrame(df_records, dtype='int')
                        print(df_postal_codes)

                        # Mapping postal codes for electricity_production_plants
                        dict_postal_codes = df_postal_codes.set_index([1]).to_dict()[0]
                        df_selected['id_postal_code'] = df_selected['id_postal_code'].map(dict_postal_codes)
                        # Null values
                        print(df_selected[df_selected['id_postal_code'].isnull() == True])
                        print('before:', len(df_selected))
                        df_selected.dropna(inplace=True)
                        # (id_postal_code)=(4147)
                        print('after:', len(df_selected))

                        # Add column Avg_monthly_production (from kw to kwh considering average occupancy)
                        df_selected.loc[:, 'Avg_monthly_production_kwh'] = np.nan
                        for row in range(0, len(df_selected)):
                            if df_selected.loc[row, 'ID_sub_category'] == 3:  # Hydro
                                df_selected.loc[row, 'Avg_monthly_production_kwh'] \
                                    = (df_selected.loc[row, 'total_power']) * 24 * 30 * 0.2922
                            elif df_selected.loc[row, 'ID_sub_category'] == 6:  # Photovoltaic
                                df_selected.loc[row, 'Avg_monthly_production_kwh'] \
                                    = (df_selected.loc[row, 'total_power']) * 24 * 30 * 0.1005
                            elif df_selected.loc[row, 'ID_sub_category'] == 1:  # Biomass
                                df_selected.loc[row, 'Avg_monthly_production_kwh'] \
                                    = (df_selected.loc[row, 'total_power']) * 24 * 30 * 0.2692
                            elif df_selected.loc[row, 'ID_sub_category'] == 8:  # Wind energy
                                df_selected.loc[row, 'Avg_monthly_production_kwh'] \
                                    = (df_selected.loc[row, 'total_power']) * 24 * 30 * 0.1875
                            else:
                                continue

                    except psycopg2.Error as e:
                        print(e)

                    # import to warehouse
                    execute_values(conn, df_selected_maincat, 'Main_categories_electricity_production')
                    execute_values(conn, df_selected_subcat, 'Sub_categories_electricity_production')
                    execute_values(conn, df_selected, 'Electricity_production_plants')

                elif records_list[table_name] == 'measuringpoints':
                    # Keep needed columns by index
                    df_selected = df.iloc[:, [0, 1, 18]]

                    # Name columns
                    df_selected.columns = ['Nr', 'Nr_measuring_point', 'Annual_average']

                    # Drop measuring points w/o location
                    df_selected = df_selected[(df_selected['Nr_measuring_point'] != 546) & \
                                              (df_selected['Nr_measuring_point'] != 642) & \
                                              (df_selected['Nr_measuring_point'] != 720) & \
                                              (df_selected['Nr_measuring_point'] != 796)]

                    # import to warehouse
                    execute_values(conn, df_selected, 'Traffic_measurement_21')

                elif records_list[table_name] == 'chargingstations_static':
                    continue

                elif records_list[table_name] == 'chargingstations_locations':
                    continue

                else:
                    continue

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()


# Lambda function execution
def lambda_handler(event, context):
    create_table_regions()
    create_table_plz()
    create_tables()
    fetch_all_data()
    return 'Import was succesful'
