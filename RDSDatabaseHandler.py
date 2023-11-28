import psycopg2
from psycopg2 import sql
import os

class RDSDatabaseHandler:

    def __init__(self):
        self.connection = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='password',
            host='cs511db.co84fzgsmbvq.us-west-2.rds.amazonaws.com',
            port='5432'
        )
        self.cursor = self.connection.cursor()                
        self.list_all_tables()        

    def list_all_tables(self):
        self.cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = self.cursor.fetchall()
        print("Tables:")
        for table in tables:
            print(table[0])

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()  # Fetch all the results of the query
            for row in results:
                print(row)  # Print each row
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()  # Rollback in case of an error

    def evaluate_query(self, query):
        explain_query = f"EXPLAIN ANALYZE {query}"
        actual_row_count = 0

        # First, execute the query to get the actual number of rows
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            actual_row_count = len(results)
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()

        # Then, execute EXPLAIN ANALYZE to get the planning and execution times
        try:
            self.cursor.execute(explain_query)
            explain_results = self.cursor.fetchall()

            planning_time = None
            execution_time = None

            for line in explain_results:
                if line[0].startswith('Planning Time:'):
                    planning_time = float(line[0].split(': ')[1].split(' ms')[0])
                if line[0].startswith('Execution Time:'):
                    execution_time = float(line[0].split(': ')[1].split(' ms')[0])

            return planning_time, execution_time, actual_row_count

        except psycopg2.Error as e:
            print(f"Error executing EXPLAIN ANALYZE: {e}")
            self.connection.rollback()
            return None, None, actual_row_count

    def execute_sql_file(self, file_path, max_commands=500):
        command = ''
        executed_count = 0

        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                command += line
                if ';' in line:
                    command = command.strip()
                    if command:
                        if executed_count >= max_commands:
                            print(f"Execution limit reached: {max_commands} commands executed.")
                            break

                        try:
                            self.execute_command(command)
                            print(f"Executed: {command[:50]}...")
                            executed_count += 1
                        except psycopg2.Error as e:
                            print(f"Error executing command: {command[:50]}...")
                            print(e)                            

                    command = ''  # Reset the command for next SQL statement

        if executed_count < max_commands:
            print(f"Execution complete. Total commands executed: {executed_count}")

    def execute_command(self, command):
        try:
            self.cursor.execute(command)
            self.connection.commit()
        except psycopg2.Error as e:
            print(f"Error executing command: {command[:50]}...")
            print(e)
            self.connection.rollback()  # Rollback to begin new transaction

    def delete_all_tables(self):
        self.cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = self.cursor.fetchall()
        
        for table in tables:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE;")
            
        self.connection.commit()
        print("All tables in the 'public' schema have been deleted.")

    def close(self):
        self.cursor.close()
        self.connection.close()