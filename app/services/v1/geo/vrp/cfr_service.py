from fastapi import APIRouter, HTTPException
from app.models.geo.vrp.cfr.cfr import CFR  # Import CFR model
import logging
from concurrent.futures import ThreadPoolExecutor
import asyncio

router = APIRouter()

# Setup basic logging
logging.basicConfig(level=logging.INFO)

# Define a global executor
executor = ThreadPoolExecutor(max_workers=10)


@router.post("/api/v1/optimize-route")
async def optimize_route(request_body: dict):
    # Example path to template file
    template_path = "app/storage/cfr/silal_main_full.json"

    # Create an instance of CFR model
    cfr_model = CFR(template_path, request_body)

    # Get the current event loop
    loop = asyncio.get_running_loop()

    # Run the blocking operation in a separate thread
    # Assuming `callCFR` is the blocking call that you want to run in a thread

    # return cfr_model.prepare_payload()
    result = await loop.run_in_executor(
        executor,
        lambda: cfr_model.callCFR(cfr_model.match_vehicles_types(cfr_model.prepare_payload()))
    )

    return result
