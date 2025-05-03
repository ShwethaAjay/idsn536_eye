"""
MongoDB API Service

This script creates a Flask API server that interfaces between ESP32 devices and MongoDB.
It enables uploading and retrieving audio files using GridFS.

To run:
pip install flask pymongo
python mongodb_api_service.py
"""

from flask import Flask, request, jsonify, send_file
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import gridfs
from bson.objectid import ObjectId
import io
import os

app = Flask(__name__)

# MongoDB connection string (from the provided code)
CONNECTION_STRING = "mongodb+srv://Shwetha:anonymeye536@cluster0.ezisqjd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def connect_to_mongodb(db_name):
    """Connect to MongoDB and return database object"""
    try:
        client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))
        # Test the connection
        client.admin.command('ping')
        app.logger.info("Connected successfully to MongoDB!")
        return client[db_name]
    except Exception as e:
        app.logger.error(f"Error connecting to MongoDB: {e}")
        return None

@app.route('/upload', methods=['POST'])
def upload_audio():
    """
    Upload audio data to MongoDB GridFS
    
    Query parameters:
    - db: Database name (default: 'Anonymeye')
    - collection: GridFS collection name (default: 'audio_files')
    
    POST body should contain raw audio bytes
    """
    try:
        # Get parameters
        db_name = request.args.get('db', 'Anonymeye')
        collection_name = request.args.get('collection', 'audio_files')
        
        # Connect to MongoDB
        db = connect_to_mongodb(db_name)
        if not db:
            return jsonify({"error": "Failed to connect to MongoDB"}), 500
        
        # Create GridFS instance
        fs = gridfs.GridFS(db, collection=collection_name)
        
        # Get audio data from request
        audio_data = request.get_data()
        if not audio_data:
            return jsonify({"error": "No audio data received"}), 400
        
        # Generate filename based on timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"audio_{timestamp}.raw"
        
        # Store in GridFS
        file_id = fs.put(audio_data, filename=filename)
        
        return jsonify({
            "status": "success",
            "file_id": str(file_id),
            "filename": filename,
            "size": len(audio_data)
        })
    
    except Exception as e:
        app.logger.error(f"Error in upload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download', methods=['GET'])
def download_audio():
    """
    Download audio data from MongoDB GridFS
    
    Query parameters:
    - db: Database name (default: 'Anonymeye')
    - collection: GridFS collection name (default: 'audio_files')
    - file_id: ID of the file to retrieve
    """
    try:
        # Get parameters
        db_name = request.args.get('db', 'Anonymeye')
        collection_name = request.args.get('collection', 'audio_files')
        file_id = request.args.get('file_id')
        
        if not file_id:
            return jsonify({"error": "No file_id provided"}), 400
        
        # Connect to MongoDB
        db = connect_to_mongodb(db_name)
        if not db:
            return jsonify({"error": "Failed to connect to MongoDB"}), 500
        
        # Create GridFS instance
        fs = gridfs.GridFS(db, collection=collection_name)
        
        # Check if file exists
        if not fs.exists(ObjectId(file_id)):
            return jsonify({"error": f"No file found with ID: {file_id}"}), 404
        
        # Get file
        grid_out = fs.get(ObjectId(file_id))
        
        # Read file data
        audio_data = grid_out.read()
        
        # Create in-memory file
        mem_file = io.BytesIO(audio_data)
        mem_file.seek(0)
        
        # Send file directly
        return send_file(
            mem_file,
            mimetype='application/octet-stream',
            as_attachment=True,
            download_name=grid_out.filename
        )
    
    except Exception as e:
        app.logger.error(f"Error in download: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/list', methods=['GET'])
def list_files():
    """
    List all audio files in the database
    
    Query parameters:
    - db: Database name (default: 'Anonymeye')
    - collection: GridFS collection name (default: 'audio_files')
    """
    try:
        # Get parameters
        db_name = request.args.get('db', 'Anonymeye')
        collection_name = request.args.get('collection', 'audio_files')
        
        # Connect to MongoDB
        db = connect_to_mongodb(db_name)
        if not db:
            return jsonify({"error": "Failed to connect to MongoDB"}), 500
        
        # Create GridFS instance
        fs = gridfs.GridFS(db, collection=collection_name)
        
        # Get all files
        files = []
        for grid_out in fs.find():
            files.append({
                "file_id": str(grid_out._id),
                "filename": grid_out.filename,
                "length": grid_out.length,
                "upload_date": grid_out.upload_date.isoformat()
            })
        
        return jsonify({"files": files})
    
    except Exception as e:
        app.logger.error(f"Error in list: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Run the Flask app on port 5000
    app.run(host='0.0.0.0', port=5001, debug=True)
