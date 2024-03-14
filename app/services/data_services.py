import pandas as pd
import json

def perform_etl_on_veterinary_terms(excel_filepath, json_filepath):
    """
    Perform ETL on the Veterinary Common Terms from an Excel file and save the cleaned data as JSON.
    
    Parameters:
    - excel_filepath (str): Path to the source Excel file.
    - json_filepath (str): Destination path for the output JSON file.
    """
    # Extract
    terms_df = pd.read_excel(excel_filepath)

    # Transform
    terms_df['Abbreviation'] = terms_df['Abbreviation'].str.upper().str.strip()
    cleaned_terms_df = terms_df.drop_duplicates(subset=['Abbreviation']).sort_values('Abbreviation')
    
    # Load
    # Note: Saving as line-separated JSON objects, one per line.
    cleaned_terms_df.to_json(json_filepath, orient='records', lines=True)
    print(f"ETL for Veterinary Common Terms completed. Data saved to {json_filepath}")

def perform_etl_on_vet_consult_dataset(csv_filepath, json_abbrev_filepath, output_csv_filepath):
    """
    Perform ETL on the Vet Consult Dataset CSV, utilizing an abbreviation dictionary, and save the cleaned data as CSV.
    
    Parameters:
    - csv_filepath (str): Path to the source Vet Consult Dataset CSV file.
    - json_abbrev_filepath (str): Path to the JSON file with the abbreviation dictionary.
    - output_csv_filepath (str): Destination path for the cleaned dataset CSV file.
    """
    # Extract
    consult_df = pd.read_csv(csv_filepath)
    
    # Load abbreviation dictionary from a line-separated JSON file
    abbrev_dict = {}
    with open(json_abbrev_filepath, 'r') as file:
        for line in file:
            obj = json.loads(line)
            abbrev_dict.update(obj)
    
    # Transform: Replace abbreviations in narratives
    def replace_abbreviations(narrative):
        for abbr, full_form in abbrev_dict.items():
            narrative = narrative.replace(abbr, full_form)
        return narrative
    
    consult_df['Narrative_cleaned'] = consult_df['Narrative'].apply(replace_abbreviations)
    
    # Load: Save the transformed dataset to a CSV file
    consult_df.to_csv(output_csv_filepath, index=False)
    print(f"ETL for Vet Consult Dataset completed. Cleaned data saved to {output_csv_filepath}")
