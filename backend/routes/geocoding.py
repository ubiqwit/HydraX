# backend/routes/geocoding.py

from flask import Blueprint, request, jsonify
from services.geocoding_service import geocode_address, GeocodingError
from services.building_service import find_nearest_building, BuildingNotFoundError
from services.rainfall_service import (
    get_current_year_rainfall_collection,
    predict_future_rainfall,
    calculate_predicted_collection
)

geocoding_bp = Blueprint("geocoding", __name__, url_prefix="/api")


@geocoding_bp.route("/geocode", methods=["POST"])
def geocode():
    """
    Expects JSON body: { "address": "123 Fake St, London" }
    Returns: { 
        "address": "...",
        "easting": ..., 
        "northing": ...,
        "building_area": ... (in sq m),
        "current_year_rainfall": {
            "year": ...,
            "rainfall_mm": ...,
            "collection_liters": ...
        },
        "predicted_rainfall": {
            year: {
                "predicted_rainfall_mm": ...,
                "predicted_collection_liters": ...
            }
        }
    } in British National Grid (OSGB36, EPSG:27700)
    or { "error": "Invalid address" }
    """
    data = request.get_json(silent=True) or {}
    address = data.get("address")

    if not address or not isinstance(address, str):
        return jsonify({"error": "address is required"}), 400

    try:
        easting, northing = geocode_address(address)
    except GeocodingError:
        return jsonify({"error": "Invalid address"}), 400
    except Exception as e:
        # Unexpected error – log in real app
        return jsonify({"error": "Internal server error"}), 500

    # Find nearest building
    try:
        building_info = find_nearest_building(easting, northing)
        building_area = building_info["area"]
    except BuildingNotFoundError:
        return jsonify({"error": "No buildings found in database"}), 500
    except Exception as e:
        return jsonify({"error": f"Error finding building: {str(e)}"}), 500

    # Get current year rainfall collection
    try:
        current_year_data = get_current_year_rainfall_collection(building_area)
    except Exception as e:
        return jsonify({"error": f"Error calculating current year rainfall: {str(e)}"}), 500

    # Predict future rainfall (next 10 years)
    try:
        predicted_rainfall = predict_future_rainfall(years_ahead=10)
        predicted_collection = calculate_predicted_collection(building_area, predicted_rainfall)
    except Exception as e:
        return jsonify({"error": f"Error predicting future rainfall: {str(e)}"}), 500

    return jsonify({
        "address": address,
        "easting": easting,
        "northing": northing,
        "building_area": building_area,
        "current_year_rainfall": current_year_data,
        "predicted_rainfall": predicted_collection
    })
