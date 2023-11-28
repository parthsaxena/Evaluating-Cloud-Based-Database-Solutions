import xml.etree.ElementTree as ET
import html
from html.parser import HTMLParser

# Configurable constants
XML_FILE_PATH = 'xml/Users.xml'
OUTPUT_SQL_FILE_PATH = 'sql/users_sql_statements.sql'
DATABASE_NAME = 'AcademiaDB'
TABLE_NAME = 'users'

class HTMLDecoder(HTMLParser):
    """Utility class to decode HTML entities to text."""
    def __init__(self):
        super().__init__()
        self.decoded_string = ""

    def handle_data(self, data):
        self.decoded_string += data

    def decode_html(self, text):
        self.decoded_string = ""
        self.feed(text)
        return self.decoded_string

def format_value(value):
    """Formats the value for SQL, decoding HTML entities, escaping special characters, and handling newlines."""
    html_decoder = HTMLDecoder()
    # Decode HTML entities
    decoded_value = html_decoder.decode_html(value)
    # Replace newlines with spaces and escape single quotes for SQL
    escaped_value = decoded_value.replace('\n', ' ').replace("'", "''")
    return f"'{escaped_value}'"

# Gather all unique column names from the XML
all_columns = set()
for event, elem in ET.iterparse(XML_FILE_PATH, events=('start',)):
    if elem.tag == 'row':
        all_columns.update(elem.attrib.keys())
    elem.clear()

# List to hold all SQL statements
sql_statements = [f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};", f"USE {DATABASE_NAME};"]

# Generate CREATE TABLE statement
columns_definitions = ', '.join(f"{column} TEXT" for column in all_columns)
sql_statements.append(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns_definitions});")

# Function to process each XML element and generate SQL
def process_element(elem, sql_statements):
    if elem.tag == 'row':
        columns = ', '.join(all_columns)
        values = ', '.join(format_value(elem.attrib.get(column, 'NULL')) for column in all_columns)
        sql = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({values});"
        sql_statements.append(sql)

# Iteratively parse the XML file for INSERT statements
for event, elem in ET.iterparse(XML_FILE_PATH, events=('end',)):
    process_element(elem, sql_statements)
    elem.clear()

# Save SQL statements to a file
with open(OUTPUT_SQL_FILE_PATH, 'w', encoding='utf-8') as file:
    for statement in sql_statements:
        file.write(statement + '\n')
