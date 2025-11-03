from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware # Import the middleware
import json
import pandas as pd

from modules.preprocessing import preprocess_hdb_data
from modules.csp_filter import csp_filter_flats
from modules.mcda_wsm import mcda_wsm
from modules.insight_generator import InsightGenerator
from modules.bayes_utils import load_bayesian_model, get_categories_from_file

# ---------------------------
# Setup
# ---------------------------
app = FastAPI()

# --- THIS IS THE MISSING PIECE ---
# Add CORS middleware to allow requests from your frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (for development)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)
# --------------------------------

# Load data once
df, _ = preprocess_hdb_data("ResaleFlatPricesData.csv", verbose=True)

# Load BN model, categories
insight_generator = InsightGenerator(load_bayesian_model("BayesianNetwork.pkl"), get_categories_from_file("CategoricalColumnsCategories.pkl"))

# Load MCDA config
with open("config/mcda_criteria.json") as f:
    criteria = json.load(f)

# ---------------------------
# Helper for weights
# ---------------------------
def get_weights(priority: str, criteria: dict) -> dict:
    if priority == "Price":
        return {"resale_price": 0.6, "floor_area_sqm": 0.2, "remaining_lease_years": 0.2}
    elif priority == "Floor Area":
        return {"resale_price": 0.2, "floor_area_sqm": 0.6, "remaining_lease_years": 0.2}
    elif priority == "Lease":
        return {"resale_price": 0.2, "floor_area_sqm": 0.2, "remaining_lease_years": 0.6}
    else:
        # Equal weights if no priority
        return {key: 1/len(criteria) for key in criteria.keys()}

# ---------------------------
# Routes
# ---------------------------

# Serve frontend index.html (This is not strictly needed anymore but good for testing)
@app.get("/")
def read_root():
    return {"status": "FlatWise API is running."}

# Recommendation API
@app.post("/recommend")
async def recommend(request: Request):
    body = await request.json()
    constraints = body.get("constraints", {})
    priority = body.get("priority", "None - treat equally")

    # Apply constraints
    filtered_df, _ = csp_filter_flats(df, constraints)    
    if filtered_df.empty:
        return JSONResponse(content={"recommendations": []})

    # Apply MCDA
    weights = get_weights(priority, criteria)
    ranked_df, _= mcda_wsm(filtered_df, criteria, weights)

    ranked_df["insight"] = ranked_df.apply(insight_generator.get_insights_on_row, axis=1)

    # Return top 10 as JSON
    top = ranked_df.head(10).to_dict(orient="records")
    return {"recommendations": top}

# Healthcheck
@app.get("/health")
async def health_check():
    return {"status": "ok"}