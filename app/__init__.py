from flask import Flask
from .services.data_services import perform_etl_on_veterinary_terms, perform_etl_on_vet_consult_dataset

def create_app():
    """
    Application factory function to create and configure the Flask app.
    """
    app = Flask(__name__)

    # Configure your Flask app here (e.g., from a config file or environment variables)
    # app.config.from_object('config.Config')

    # Register a custom command to run ETL
    @app.cli.command("run-etl")
    def run_etl():
        """
        A custom Flask CLI command to execute the ETL process.
        """
        print("Starting the ETL process...")
        
        # Define file paths for ETL
        common_terms_path = 'data_files/Veterinary Common Terms.xlsx'
        json_terms_path = 'data_files/cleaned_veterinary_common_terms.json'
        consult_csv_path = 'data_files/Vet Consult Dataset.csv'
        cleaned_consult_csv_path = 'data_files/cleaned_vet_consult_dataset.csv'
        
        # Execute ETL functions
        perform_etl_on_veterinary_terms(common_terms_path, json_terms_path)
        perform_etl_on_vet_consult_dataset(consult_csv_path, json_terms_path, cleaned_consult_csv_path)
        
        print("ETL process completed.")
    
    # Register blueprints or extensions if you have any
    # app.register_blueprint(some_blueprint)

    return app
