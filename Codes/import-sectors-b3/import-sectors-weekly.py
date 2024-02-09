import requests
import zipfile
import io
import pandas as pd

# Function to check for non-blank, four-character words
def is_four_characters(word):
    return isinstance(word, str) and word.strip() and len(word.strip()) == 4

# Set pandas options to display all rows (use with caution for very large data sets)
pd.set_option('display.max_rows', None)

# Send a GET request to the URL
url = 'https://www.b3.com.br/data/files/57/E6/AA/A1/68C7781064456178AC094EA8/ClassifSetorial.zip'
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Use io.BytesIO to treat the downloaded data as a file-like object for zipfile
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the Excel file
        excel_names = [name for name in thezip.namelist() if name.endswith('.xlsx') or name.endswith('.xls')]

        if excel_names:
            # Assuming there's only one Excel file in the zip, use its name
            excel_file_name = excel_names[0]

            # Read the Excel file directly into pandas without using the first row as headers
            with thezip.open(excel_file_name) as excel_file:
                df = pd.read_excel(excel_file, header=None)

            # Assign column names based on their position
            df.columns = ['EconomicSector', 'Subsector', 'C', 'D', 'E']

            # Logic for updating 'EconomicSector' and 'Subsector' values
            condition_economic_sector = df['Subsector'].notnull() & df['C'].notnull()
            df['EconomicSector_Update'] = df.apply(lambda row: row['Subsector'] if condition_economic_sector[row.name] else None, axis=1)
            df['EconomicSector_Update'] = df['EconomicSector_Update'].ffill()
            df['EconomicSector'] = df.apply(lambda row: row['EconomicSector_Update'] if is_four_characters(row['D']) else row['EconomicSector'], axis=1)
            
            condition_subsector = df['EconomicSector'].notnull() & df['C'].notnull()
            df['Subsector_Update'] = df.apply(lambda row: row['EconomicSector'] if condition_subsector[row.name] else None, axis=1)
            df['Subsector_Update'] = df['Subsector_Update'].ffill()
            df['Subsector'] = df.apply(lambda row: row['Subsector_Update'] if is_four_characters(row['D']) else row['Subsector'], axis=1)

            # Drop the temporary update columns
            df.drop(columns=['EconomicSector_Update', 'Subsector_Update'], inplace=True)

            # Forward fill the 'Segment' column based on 'C' and 'D'
            df['Segment'] = df.apply(lambda row: row['C'] if not is_four_characters(row['D']) else None, axis=1)
            df['Segment'] = df['Segment'].ffill()

            # Filter the DataFrame to only include rows where 'D' has a 4-character word
            df_filtered = df[df['D'].apply(is_four_characters)]

            # Reorder the DataFrame columns
            df_final = df_filtered[['D', 'EconomicSector', 'Subsector', 'Segment', 'E']]

            # Print the final DataFrame
            print(df_final)  # This will print all rows of the final DataFrame
        else:
            print("No Excel file found in the ZIP archive.")
else:
    print("Failed to download the file")
