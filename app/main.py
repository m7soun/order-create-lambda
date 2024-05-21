from dotenv import load_dotenv
from fastapi import FastAPI
from starlette.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from google.cloud import optimization_v1
import json
import os
from fastapi import File, UploadFile, HTTPException
import logging
from typing import List, Dict
import requests
from .services.v1.files.parsers.files_parser_service import router as router_files_parser
from .services.v1.geo.vrp.cfr_service import router as cfr_router

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_key.json"

logger = logging.getLogger(__name__)
app.include_router(router_files_parser)
app.include_router(cfr_router)
load_dotenv(".env")








def call_eta_api(start_lat, start_lon, stop_lat, stop_lon, country):
    endpoint = "_enver_"
#     headers = {
#         'Accept': 'application/json',
#         'Content-Type': 'application/json',
#     }
#     query_params = {
#         'country': country,
#         'start_lon': start_lon,
#         'stop_lon': stop_lon,
#         'start_lat': start_lat,
#         'stop_lat': stop_lat,
#         'source': 'mobile',
#         'action': 'GetAll'
    }
    response = requests.get(endpoint, headers=headers, params=query_params)
    return response.json()
