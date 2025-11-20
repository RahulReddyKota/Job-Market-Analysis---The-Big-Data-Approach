from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from pathlib import Path
import uvicorn

app = FastAPI(title="Job Market API", version="1.0.0")

class CareerPlanRequest(BaseModel):
    current_skills: List[str]
    target_role: Optional[str] = None

class SalaryRequest(BaseModel):
    years_experience: int
    skills: List[str] = []
    currency: str = "USD"
    period: str = "YEARLY"

# Load skills data once at startup
SKILLS_DATA = None

def load_skills_data():
    global SKILLS_DATA
    if SKILLS_DATA is None:
        skills_path = Path("data/gold/skills_demand.parquet")
        if skills_path.exists():
            try:
                SKILLS_DATA = pd.read_parquet(skills_path)
            except Exception:
                SKILLS_DATA = pd.DataFrame()
        else:
            SKILLS_DATA = pd.DataFrame()
    return SKILLS_DATA

@app.on_event("startup")
async def startup_event():
    load_skills_data()

@app.get("/health")
def health():
    return {"ok": True, "status": "running"}

@app.post("/career-recommendations")
def career_recommendations(req: CareerPlanRequest):
    df = load_skills_data()
    
    if df.empty or not {"skill","count"}.issubset(df.columns):
        return {"error": "Skills data not available"}
    
    have = set(s.lower() for s in req.current_skills)
    candidates = (
        df.sort_values("count", ascending=False)["skill"].astype(str).str.lower().tolist()
    )
    recs = [s for s in candidates if s not in have][:20]
    
    return {
        "target_role": req.target_role,
        "recommended_skills": recs,
        "total_skills_analyzed": len(candidates),
        "your_skills": list(have)
    }

@app.post("/predict-salary")
def predict_salary(req: SalaryRequest):
    # Simple salary prediction based on experience
    base_salary = 50000
    experience_multiplier = 1 + (req.years_experience * 0.1)
    predicted = base_salary * experience_multiplier
    
    return {
        "predicted_salary": int(predicted),
        "lower_bound": int(predicted * 0.8),
        "upper_bound": int(predicted * 1.2),
        "currency": req.currency,
        "period": req.period
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)






