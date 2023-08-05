import json
from flask import Flask, jsonify, request
import pandas as pd
import logging

app = Flask(__name__)

# Initialize the logger
logging.basicConfig(filename='api.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# allow files smaller than 100 mb 
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 

@app.before_request
def log_request_info():
    # Log the request details
    logging.info(f"Request: {request.method} {request.url} - Form Data: {request.form}")

@app.route('/health/', methods=['GET'])
def ok_generator():    
    return jsonify("OK!")

@app.route('/stats/<string:column_name>/<string:separator>/', methods=['POST'])
def csv_maker(column_name,separator):
    try:
        # check whether given column name is a string
        if column_name == None or type(column_name)!= str :
            return jsonify({"error":"Column name is not given correctly"}), 401
        
        # check wheter a seperator is given (example: ';')
        if separator == "" or separator == None:
            return jsonify({"error": "The seperator is not given correctly"}), 402
        
        # check whether a file is given 
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 403
        
        file = request.files['file']
        
        # check whether the given file is a csv  
        if not file.filename.endswith('.csv'):
            return jsonify({"error": "Invalid file format. Expected a CSV file"}), 404
        
        df = pd.read_csv(file, sep=separator, encoding='iso-8859-1', index_col=None)
        
        # check whether the given csv contains both 'PID', 'Zeitindex'columns
        if not {'PID', 'Zeitindex'}.issubset(df.columns):
            return jsonify({"error": "File does not contain one of these columns: 'PID', 'Zeitindex'"}), 405 
        
        # check whether the given csv contains the given column
        if not column_name in df.columns:
            return jsonify({"error": f"File does not contain the entered column: {column_name}"}), 406 
        
        # calculate the needed stats
        stats_result = stats_calculator(df, column_name)
        
        return stats_result
    
    except Exception as e:
        logging.exception("An error occurred during the /stats/ request")
        return jsonify({"error": str(e)}), 500
    
def stats_calculator(df, column_name):
    # remove any non-numeric characters from the entered column
    df[column_name] = df[column_name].str.replace(r'\D', '', regex=True)
    
    # change the column into integer values
    df[column_name] = df[column_name].astype(int)

    # calculate "Summe pro Zeiteinheit"
    SPZ = df.groupby("Zeitindex")[column_name].sum()
    
    # calculate "Durchschnitt pro Zeiteinheit"
    DPZ = df.groupby("Zeitindex")[column_name].mean()
    
    # Convert Series to dictionaries
    SPZ_dict = SPZ.to_dict()
    DPZ_dict = DPZ.to_dict()
    
    return jsonify({"Summe pro Zeiteinheit": SPZ_dict,"Durchschnitt pro Zeiteinheit": DPZ_dict}), 200


app.run()
