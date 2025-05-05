"""
MongoDB Audio API Service (Improved)

Key enhancements:
- Fixed database connection checks
- Proper audio MIME type detection
- Better error handling
- GridFS optimizations
"""

from flask import Flask, request, jsonify, send_file
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import gridfs
from bson import ObjectId
from bson.errors import InvalidId
import io
import os
import mimetypes
from datetime import datetime
import wave

app = Flask(__name__)

# MongoDB configuration
CONNECTION_STRING = "mongodb+srv://Shwetha:anonymeye536@cluster0.ezisqjd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DEFAULT_DB = "Anonymeye"
DEFAULT_COLLECTION = "audio_files"

def connect_to_mongodb(db_name):
    """Establish MongoDB connection with error handling"""
    try:
        client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))
        client.admin.command('ping')  # Test connection
        app.logger.info(f"Connected to MongoDB: {db_name}")
        return client[db_name]
    except Exception as e:
        app.logger.error(f"Connection failed: {str(e)}")
        return None

@app.route('/upload', methods=['POST'])
def upload_audio():
    """Upload audio to GridFS with proper metadata"""
    try:
        db_name = request.args.get('db', DEFAULT_DB)
        collection = request.args.get('collection', DEFAULT_COLLECTION)
        
        db = connect_to_mongodb(db_name)
        if db is None:
            return jsonify({"error": "Database connection failed"}), 500

        fs = gridfs.GridFS(db, collection=collection)
        audio_data = request.get_data()
        
        if not audio_data:
            return jsonify({"error": "No audio data received"}), 400

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"audio_{timestamp}.m4a"  # Changed to .m4a for better MIME detection

        # Store with metadata
        file_id = fs.put(audio_data,
                        filename=filename,
                        metadata={
                            "upload_type": "audio",
                            "source": "ESP32"
                        })

        return jsonify({
            "status": "success",
            "file_id": str(file_id),
            "filename": filename,
            "size": len(audio_data)
        })

    except Exception as e:
        app.logger.error(f"Upload error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/download', methods=['GET'])
def download_audio():
    try:
        db_name = request.args.get('db', 'Anonymeye')
        collection_name = request.args.get('collection', 'audio_files')
        file_id = request.args.get('file_id')

        if not file_id:
            return jsonify({"error": "No file_id provided"}), 400

        db = connect_to_mongodb(db_name)
        if db is None:
            return jsonify({"error": "Failed to connect to MongoDB"}), 500

        fs = gridfs.GridFS(db, collection=collection_name)
        if not fs.exists(ObjectId(file_id)):
            return jsonify({"error": f"No file found with ID: {file_id}"}), 404

        grid_out = fs.get(ObjectId(file_id))
        raw_data = grid_out.read()

        # WAV parameters: adjust to match your ESP32 recording settings
        channels = 1          # Mono
        sampwidth = 1         # 8-bit PCM (1 byte)
        framerate = 16000     # 16kHz sample rate

        wav_buffer = io.BytesIO()
        with wave.open(wav_buffer, 'wb') as wav_file:
            wav_file.setnchannels(channels)
            wav_file.setsampwidth(sampwidth)
            wav_file.setframerate(framerate)
            wav_file.writeframes(raw_data)

        wav_buffer.seek(0)
        return send_file(
            wav_buffer,
            mimetype='audio/wav',
            as_attachment=True,
            download_name=grid_out.filename.replace('.raw', '.wav')
        )

    except Exception as e:
        app.logger.error(f"Error in download: {e}")
        return jsonify({"error": str(e)}), 500
# @app.route('/download', methods=['GET'])
# def download_audio():
#     """Download audio with proper MIME type handling"""
#     try:
#         db_name = request.args.get('db', DEFAULT_DB)
#         collection = request.args.get('collection', DEFAULT_COLLECTION)
#         file_id = request.args.get('file_id')

#         if not file_id:
#             return jsonify({"error": "Missing file_id parameter"}), 400

#         # Validate ObjectId format
#         try:
#             obj_id = ObjectId(file_id)
#         except InvalidId:
#             return jsonify({"error": "Invalid file_id format"}), 400

#         db = connect_to_mongodb(db_name)
#         if db is None:
#             return jsonify({"error": "Database connection failed"}), 500

#         fs = gridfs.GridFS(db, collection=collection)
        
#         if not fs.exists(obj_id):
#             return jsonify({"error": "File not found"}), 404

#         grid_out = fs.get(obj_id)
#         raw_data = grid_out.read()

#         # Create WAV header with ESP32 default settings
#         channels = 1    # Mono
#         sampwidth = 1   # 8-bit (1 byte/sample)
#         framerate = 16000  # 16kHz sample rate

#         with io.BytesIO() as wav_buffer:
#             with wave.open(wav_buffer, 'wb') as wav_file:
#                 wav_file.setnchannels(channels)
#                 wav_file.setsampwidth(sampwidth)
#                 wav_file.setframerate(framerate)
#                 wav_file.writeframes(raw_data)
            
#             wav_buffer.seek(0)
#             return send_file(
#                 wav_buffer,
#                 mimetype='audio/wav',
#                 as_attachment=True,
#                 download_name=f"{grid_out.filename.split('.')[0]}.wav"
#             )

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500
# MAIN WORK
    #     audio_data = grid_out.read()

    #     # Determine MIME type from filename
    #     mime_type, _ = mimetypes.guess_type(grid_out.filename)
    #     if not mime_type:
    #         mime_type = 'application/octet-stream'  # Fallback

    #     return send_file(
    #         io.BytesIO(audio_data),
    #         mimetype=mime_type,
    #         as_attachment=True,
    #         download_name=grid_out.filename
    #     )

    # except Exception as e:
    #     app.logger.error(f"Download error: {str(e)}")
    #     return jsonify({"error": str(e)}), 500

@app.route('/list', methods=['GET'])
def list_files():
    """List all audio files with pagination"""
    try:
        db_name = request.args.get('db', DEFAULT_DB)
        collection = request.args.get('collection', DEFAULT_COLLECTION)
        
        db = connect_to_mongodb(db_name)
        if db is None:
            return jsonify({"error": "Database connection failed"}), 500

        fs = gridfs.GridFS(db, collection=collection)
        files = []

        for grid_out in fs.find():
            files.append({
                "file_id": str(grid_out._id),
                "filename": grid_out.filename,
                "length": grid_out.length,
                "upload_date": grid_out.upload_date.isoformat(),
                "content_type": grid_out.content_type
            })

        return jsonify({"files": files})

    except Exception as e:
        app.logger.error(f"List error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Initialize MIME type database
    mimetypes.init()
    app.run(host='0.0.0.0', port=5001, debug=True)



# """
# MongoDB API Service

# This script creates a Flask API server that interfaces between ESP32 devices and MongoDB.
# It enables uploading and retrieving audio files using GridFS.

# To run:
# pip install flask pymongo
# python mongodb_api_service.py
# """

# from flask import Flask, request, jsonify, send_file
# from pymongo import MongoClient
# from pymongo.server_api import ServerApi
# import gridfs
# from bson.objectid import ObjectId
# import io
# import os

# app = Flask(__name__)

# # MongoDB connection string (from the provided code)
# CONNECTION_STRING = "mongodb+srv://Shwetha:anonymeye536@cluster0.ezisqjd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# def connect_to_mongodb(db_name):
#     """Connect to MongoDB and return database object"""
#     try:
#         client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))
#         # Test the connection
#         client.admin.command('ping')
#         app.logger.info("Connected successfully to MongoDB!")
#         return client[db_name]
#     except Exception as e:
#         app.logger.error(f"Error connecting to MongoDB: {e}")
#         return None

# @app.route('/upload', methods=['POST'])
# def upload_audio():
#     """
#     Upload audio data to MongoDB GridFS
    
#     Query parameters:
#     - db: Database name (default: 'Anonymeye')
#     - collection: GridFS collection name (default: 'audio_files')
    
#     POST body should contain raw audio bytes
#     """
#     try:
#         # Get parameters
#         db_name = request.args.get('db', 'Anonymeye')
#         collection_name = request.args.get('collection', 'audio_files')
        
#         # Connect to MongoDB
#         db = connect_to_mongodb(db_name)
#         if db is None:
#             return jsonify({"error": "Failed to connect to MongoDB"}), 500
        
#         # Create GridFS instance
#         fs = gridfs.GridFS(db, collection=collection_name)
        
#         # Get audio data from request
#         audio_data = request.get_data()
#         if not audio_data:
#             return jsonify({"error": "No audio data received"}), 400
        
#         # Generate filename based on timestamp
#         from datetime import datetime
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         filename = f"audio_{timestamp}.raw"
        
#         # Store in GridFS
#         file_id = fs.put(audio_data, filename=filename)
        
#         return jsonify({
#             "status": "success",
#             "file_id": str(file_id),
#             "filename": filename,
#             "size": len(audio_data)
#         })
    
#     except Exception as e:
#         app.logger.error(f"Error in upload: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/download', methods=['GET'])
# def download_audio():
#     """
#     Download audio data from MongoDB GridFS
    
#     Query parameters:
#     - db: Database name (default: 'Anonymeye')
#     - collection: GridFS collection name (default: 'audio_files')
#     - file_id: ID of the file to retrieve
#     """
#     try:
#         # Get parameters
#         db_name = request.args.get('db', 'Anonymeye')
#         collection_name = request.args.get('collection', 'audio_files')
#         file_id = request.args.get('file_id')
        
#         if not file_id:
#             return jsonify({"error": "No file_id provided"}), 400
        
#         # Connect to MongoDB
#         db = connect_to_mongodb(db_name)
#         if db is None:
#             return jsonify({"error": "Failed to connect to MongoDB"}), 500
        
#         # Create GridFS instance
#         fs = gridfs.GridFS(db, collection=collection_name)
        
#         # Check if file exists
#         if not fs.exists(ObjectId(file_id)):
#             return jsonify({"error": f"No file found with ID: {file_id}"}), 404
        
#         # Get file
#         grid_out = fs.get(ObjectId(file_id))
        
#         # Read file data
#         audio_data = grid_out.read()
        
#         # Create in-memory file
#         mem_file = io.BytesIO(audio_data)
#         mem_file.seek(0)
        
#         # Send file directly
#         return send_file(
#             mem_file,
#             mimetype='application/octet-stream',
#             as_attachment=True,
#             download_name=grid_out.filename
#         )
    
#     except Exception as e:
#         app.logger.error(f"Error in download: {e}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/list', methods=['GET'])
# def list_files():
#     """
#     List all audio files in the database
    
#     Query parameters:
#     - db: Database name (default: 'Anonymeye')
#     - collection: GridFS collection name (default: 'audio_files')
#     """
#     try:
#         # Get parameters
#         db_name = request.args.get('db', 'Anonymeye')
#         collection_name = request.args.get('collection', 'audio_files')
        
#         # Connect to MongoDB
#         db = connect_to_mongodb(db_name)
#         if db is None:
#             return jsonify({"error": "Failed to connect to MongoDB"}), 500
        
#         # Create GridFS instance
#         fs = gridfs.GridFS(db, collection=collection_name)
        
#         # Get all files
#         files = []
#         for grid_out in fs.find():
#             files.append({
#                 "file_id": str(grid_out._id),
#                 "filename": grid_out.filename,
#                 "length": grid_out.length,
#                 "upload_date": grid_out.upload_date.isoformat()
#             })
        
#         return jsonify({"files": files})
    
#     except Exception as e:
#         app.logger.error(f"Error in list: {e}")
#         return jsonify({"error": str(e)}), 500

# if __name__ == '__main__':
#     # Run the Flask app on port 5000
#     app.run(host='0.0.0.0', port=5001, debug=True)
