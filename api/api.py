import uuid
from typing import Dict

import uvicorn
from fastapi import FastAPI, status, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from api_kafka_integration.kafka_api_util import copytokafkaobj
from infra import logging as logger
from models.person import Person

app = FastAPI()


@app.get('/healthy', status_code=status.HTTP_200_OK)
def health():
    return {'message': 'Healthy'}


@app.post("/register", status_code=status.HTTP_201_CREATED)
async def register_person(person: Person) -> Dict[str, uuid.UUID]:
    print(f'Input data => {person}')
    result = await copytokafkaobj(person)
    return {'id': result}


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print(f'"detail": {exc.errors()}')
    print(f'"body": {exc.body}')
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )


if __name__ == '__main__':
    logger.info("Starting APP")
    uvicorn.run(app)
