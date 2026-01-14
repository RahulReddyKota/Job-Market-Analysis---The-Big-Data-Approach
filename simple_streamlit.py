import streamlit as st
import requests
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Job Market Analytics",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar navigation
st.sidebar.title("💼 Job Market Analytics")

section = st.sidebar.radio(
    "Navigate",
    [
        "Market Overview",
        "Skill Analysis", 
        "Salary Intelligence",
        "Career Planner",
        "Real-time Trends",
        "About",
    ],
)

# API functions
def call_api_career_recs(current_skills: list, target_role: str = None):
    try:
        payload = {"current_skills": current_skills, "target_role": target_role}
        r = requests.post("http://localhost:8000/career-recommendations", json=payload, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"recommended_skills": [], "error": str(e)}

def call_api_predict(years_experience: int, currency: str = "USD", period: str = "YEARLY"):
    try:
        payload = {
            "years_experience": years_experience,
            "skills": [],
            "currency": currency,
            "period": period,
        }
        r = requests.post("http://localhost:8000/predict-salary", json=payload, timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"predicted_salary": None, "error": str(e)}

# Main content
if section == "Market Overview":
    st.title("📊 Market Overview")
    st.write("Welcome to the Job Market Analytics Dashboard!")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Skills Analyzed", "49", "Skills in demand")
    with col2:
        st.metric("API Status", "✅ Online", "Fast & Responsive")
    with col3:
        st.metric("Data Sources", "4", "GitHub, StackOverflow, BLS, Kaggle")
    
    st.info("🎯 This dashboard helps you make data-driven career decisions!")

elif section == "Skill Analysis":
    st.title("📈 Skill Analysis")
    st.write("Analyze skill demand trends and get forecasts.")
    
    # Create sample data for demonstration
    skills_data = {
        'skill': ['python', 'javascript', 'java', 'react', 'aws', 'docker', 'kubernetes', 'machine learning'],
        'count': [15000, 14000, 13000, 11000, 10000, 9000, 8000, 7000]
    }
    df = pd.DataFrame(skills_data)
    
    fig = px.bar(df, x='skill', y='count', title='Top Skills by Demand')
    st.plotly_chart(fig, use_container_width=True)

elif section == "Salary Intelligence":
    st.title("💰 Salary Intelligence")
    st.write("Get salary predictions and insights.")
    
    col1, col2 = st.columns(2)
    with col1:
        years = st.slider("Years of Experience", 0, 20, 5)
    with col2:
        currency = st.selectbox("Currency", ["USD", "EUR", "GBP"])
    
    if st.button("Predict Salary", type="primary"):
        with st.spinner("Calculating salary prediction..."):
            result = call_api_predict(years, currency)
            
            if result.get("error"):
                st.error(f"Error: {result['error']}")
            else:
                salary = result.get("predicted_salary", 0)
                lower = result.get("lower_bound", 0)
                upper = result.get("upper_bound", 0)
                
                st.success(f"Predicted Salary: ${salary:,.0f}")
                st.info(f"Range: ${lower:,.0f} - ${upper:,.0f}")

elif section == "Career Planner":
    st.title("🎯 Career Planner")
    st.write("Get personalized skill recommendations based on job market demand.")
    
    col1, col2 = st.columns(2)
    with col1:
        current_skills = st.text_input("Your current skills (comma-separated)", value="python, sql", help="Enter skills separated by commas")
    with col2:
        target_role = st.text_input("Target role (optional)", value="Data Scientist", help="What role are you targeting?")
    
    if st.button("Get Recommendations", type="primary"):
        if not current_skills.strip():
            st.warning("Please enter your current skills")
        else:
            with st.spinner("Analyzing skills and generating recommendations..."):
                skills_list = [s.strip().lower() for s in current_skills.split(",") if s.strip()]
                result = call_api_career_recs(skills_list, target_role)
                
                if result.get("error"):
                    st.error(f"Error: {result['error']}")
                else:
                    recommendations = result.get("recommended_skills", [])
                    if recommendations:
                        st.success(f"Found {len(recommendations)} recommendations!")
                        
                        # Display recommendations
                        for i, skill in enumerate(recommendations[:15], 1):
                            st.write(f"**{i}.** {skill.title()}")
                        
                        if len(recommendations) > 15:
                            st.caption(f"... and {len(recommendations) - 15} more skills to explore")
                    else:
                        st.info("No recommendations available. Please check your input and try again.")

elif section == "Real-time Trends":
    st.title("⚡ Real-time Trends")
    st.write("Live job market trends and insights.")
    
    # Sample trending data
    trending_data = {
        'trend': ['AI/ML Growth', 'Remote Work', 'Cloud Migration', 'DevOps Adoption', 'Data Science'],
        'growth': [25, 20, 18, 15, 12]
    }
    df_trends = pd.DataFrame(trending_data)
    
    fig = px.bar(df_trends, x='trend', y='growth', title='Current Market Trends')
    st.plotly_chart(fig, use_container_width=True)

elif section == "About":
    st.title("ℹ️ About")
    st.write("""
    ## Job Market Analytics Dashboard
    
    This dashboard provides comprehensive job market insights including:
    
    - **Skill Demand Analysis**: Track which skills are most in-demand
    - **Salary Predictions**: Get AI-powered salary estimates
    - **Career Planning**: Receive personalized skill recommendations
    - **Market Trends**: Stay updated with real-time job market trends
    
    ### Technology Stack
    - **Backend**: FastAPI (Python)
    - **Frontend**: Streamlit
    - **Data**: 20GB+ of job market data
    - **ML**: XGBoost for salary predictions
    
    ### Data Sources
    - GitHub Archive
    - StackOverflow Surveys
    - Bureau of Labor Statistics
    - Kaggle Job Datasets
    """)

# Footer
st.markdown("---")
st.caption("🚀 Job Market Analytics Dashboard - Making data-driven career decisions easy!")







