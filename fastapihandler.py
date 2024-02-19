import json
from pydantic import BaseModel
from model.DataManager import DataManager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()

@app.get("/api")
async def api():
    man = DataManager()
    lst=man.search('1', page_size=50, order_by='data_time DESC')
    return lst

@app.get("/hi")
async def hi():
    return "hello"

@app.get("/api/getstate")
async def get_state():
    with open('state.json', 'r') as f:
        state = json.load(f)
    return state

class StateIn(BaseModel):
    pump: str
    uvlamp: str

@app.post("/api/state")
async def update_state(state_in: StateIn):
    pump = state_in.pump
    uvlamp = state_in.uvlamp
    print("API State",pump, uvlamp)
    state = {"pump": pump, "uvlamp": uvlamp}
    with open('userstate.json', 'w') as f:
        json.dump(state, f)
    return state

app.mount("/", StaticFiles(directory="./react" , html=True), name="ReactPage")
