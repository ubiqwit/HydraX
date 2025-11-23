# backend/app.py

from flask import Flask
from flask_cors import CORS
from routes.geocoding import geocoding_bp

def create_app():
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes
    # register blueprints
    app.register_blueprint(geocoding_bp)
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)