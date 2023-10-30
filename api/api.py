import uvicorn
from fastapi import FastAPI, status
from infra import logging as logger
from models.person import Person

app = FastAPI()


@app.get('/healthy', status_code=status.HTTP_200_OK)
def health():
    return {'message': 'Healthy'}


@app.post("/register", status_code=status.HTTP_201_CREATED)
def register_person(person: Person):
    return {'response':person}


if __name__ == '__main__':
    logger.info("Starting APP")
    uvicorn.run(app)
