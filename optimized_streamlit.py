import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from pathlib import Path
import time

# Page config
st.set_page_config(
    page_title="Smart Career Planner",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Cache API calls for better performance
@st.cache_data(ttl=300)  # Cache for 5 minutes
def call_career_api(skills, target_role):
    try:
        response = requests.post(
            "http://127.0.0.1:8001/career-recommendations",
            json={"current_skills": skills, "target_role": target_role},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

# Helper function to safely convert salary to numeric
def safe_numeric_salary(df, column='salary_amount'):
    """Convert salary column to numeric, handling errors"""
    if df.empty or column not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[column], errors='coerce')

# Currency conversion rates (approximate, as of 2024)
CURRENCY_RATES = {
    'USD': 1.0,
    'EUR': 1.08,
    'GBP': 1.27,
    'CAD': 0.73,
    'AUD': 0.66,
    'BBD': 0.50,  # Barbadian Dollar
}

def convert_to_yearly_usd(salary, currency, period):
    """Convert salary to yearly USD for proper comparison"""
    if pd.isna(salary) or salary <= 0:
        return None
    
    # Convert to USD first
    rate = CURRENCY_RATES.get(str(currency).upper(), 1.0)
    salary_usd = float(salary) * rate
    
    # Convert to yearly
    period_upper = str(period).upper()
    if period_upper == 'YEARLY' or period_upper == 'YEAR' or period_upper == 'ANNUAL':
        return salary_usd
    elif period_upper == 'HOURLY' or period_upper == 'HOUR':
        # Assume 2080 hours per year (40 hours/week * 52 weeks)
        return salary_usd * 2080
    elif period_upper == 'MONTHLY' or period_upper == 'MONTH':
        return salary_usd * 12
    elif period_upper == 'WEEKLY' or period_upper == 'WEEK':
        return salary_usd * 52
    elif period_upper == 'BIWEEKLY' or period_upper == 'BI-WEEKLY':
        return salary_usd * 26
    else:
        # Default to yearly if unknown
        return salary_usd

def normalize_salaries_to_yearly_usd(df):
    """Normalize all salaries to yearly USD for comparison"""
    if df.empty or 'salary_amount' not in df.columns:
        return df.copy()
    
    df_copy = df.copy()
    numeric_salaries = safe_numeric_salary(df_copy)
    df_copy['salary_amount_numeric'] = numeric_salaries
    
    # Convert to yearly USD
    currency_col = df_copy.get('currency', pd.Series(['USD'] * len(df_copy)))
    period_col = df_copy.get('period', pd.Series(['YEARLY'] * len(df_copy)))
    
    df_copy['salary_yearly_usd'] = df_copy.apply(
        lambda row: convert_to_yearly_usd(
            row.get('salary_amount_numeric'),
            row.get('currency', 'USD'),
            row.get('period', 'YEARLY')
        ), axis=1
    )
    
    # Filter out invalid conversions
    df_copy = df_copy[df_copy['salary_yearly_usd'].notna() & (df_copy['salary_yearly_usd'] > 0)]
    
    # Filter out unrealistic salaries (too low or too high)
    df_copy = df_copy[(df_copy['salary_yearly_usd'] >= 20000) & (df_copy['salary_yearly_usd'] <= 500000)]
    
    return df_copy

# Job title filtering to remove noise
TITLE_NOISE = {
    "religion", "racial", "color", "gender", "disability", "veteran", "eeo", "equal",
    "vision", "dental", "medical", "401k", "hourly", "yearly", "weekly", "benefit", "benefits",
    "drug", "background", "check", "sign", "bonus", "insurance", "policy", "policies", "pto",
    "training", "train", "trainings", "html", "engineering", "persons with disabilities",
    "paid time off", "available budget", "and abilities", "skills", "experience", "abilities",
    "ability", "housing expenses", "accomplishing", "daily tasks", "around the store",
    "email", "maintenance", "responsibilities", "operating", "instructions", "colleagues",
    "contact", "recruiting", "accommodation", "confidential", "eligibility", "recruiter",
    "including", "those with", "accommodate", "applicants", "needs", "afternoon", "evening",
    "availability", "physical ability", "prolonged standing", "beacon", "innovation"
}

# Phrases that indicate this is NOT a job title
TITLE_NOISE_PHRASES = {
    "and abilities", "housing expenses", "daily tasks", "around the store", "accomplishing",
    "email maintenance", "operating and maintenance", "contact capital one", "recruiting at",
    "accommodation at", "confidential and will", "eligibility and provide", "recruiter will",
    "including those with", "accommodate applicants", "afternoon and evening", "physical ability to",
    "prolonged standing", "beacon of innovation", "colleagues from more than", "developing bearing",
    "net zero greenhouse", "production facilities by", "million through our", "nourishing neighbors",
    "living in our communities", "impacted by disasters", "enough to eat", "self-starter with",
    "goal-oriented with", "lifelong quest", "personal development", "career growth", "persuasive communication",
    "oral & written", "comfortable speaking", "front of groups", "top-notch consultative",
    "comfortable marketing", "c-level executives", "superior time management", "desire to work",
    "home-based office", "professional appearance", "passion to market", "saas product",
    "solves a small business", "most vexing problem", "and professional associations",
    "forming partnerships", "chambers of commerce", "in-person and virtual", "delivering business",
    "credit seminars", "groups of 5-50", "business owners", "monitoring and improving",
    "sales performance", "direct reports", "developing sales strategies", "managing the sales",
    "sales process", "leading team meetings", "meeting monthly"
}

# Tech-specific keywords - titles must contain at least one of these
TECH_KEYWORDS = {
    "engineer", "developer", "scientist", "analyst", "architect", "programmer", "coder",
    "qa", "sdet", "security", "devops", "cloud", "ml", "machine learning", "ai", "artificial intelligence",
    "data", "database", "bi", "business intelligence", "backend", "front end", "frontend",
    "full stack", "ios", "android", "mobile", "platform", "infra", "infrastructure", "site reliability", "sre",
    "software", "systems", "network", "cyber", "information", "it", "tech", "technology",
    "python", "java", "javascript", "react", "node", "angular", "vue", "sql", "aws", "azure", "gcp",
    "tensorflow", "pytorch", "kubernetes", "docker", "jenkins", "terraform", "ansible",
    "web", "api", "microservices", "blockchain", "crypto", "fintech", "saas", "paas", "iaas"
}

# Non-tech keywords that should be excluded even if they appear with tech keywords
NON_TECH_KEYWORDS = {
    "store", "retail", "sales", "marketing", "customer service", "cashier", "merchandise",
    "warehouse", "logistics", "supply chain", "inventory", "shipping", "receiving",
    "restaurant", "food service", "hospitality", "hotel", "catering", "chef", "cook",
    "nurse", "medical", "healthcare", "doctor", "physician", "therapist", "dental",
    "teacher", "education", "instructor", "professor", "academic", "curriculum",
    "real estate", "property", "leasing", "broker", "agent",
    "fitness", "trainer", "gym", "wellness", "personal trainer",
    "beauty", "salon", "cosmetology", "stylist", "barber",
    "automotive", "mechanic", "technician"  # Note: "technician" is ambiguous, but we'll filter by context
}

TITLE_ALLOW_KEYWORDS = TECH_KEYWORDS  # Use tech keywords only

def is_valid_job_title(title):
    """Check if a job title is valid (not noise)"""
    if pd.isna(title):
        return False
    
    title_original = str(title).strip()
    title_str = title_original.lower()
    
    # Too short or too long (job titles are typically 5-80 characters)
    if len(title_str) < 5 or len(title_str) > 80:
        return False
    
    # Contains sentence-ending punctuation (likely a description, not a title)
    if any(punct in title_original for punct in ['.', '!', '?', '•', ':', ';']):
        return False
    
    # Contains noise phrases (longer phrases that indicate descriptions)
    for phrase in TITLE_NOISE_PHRASES:
        if phrase in title_str:
            return False
    
    # Contains noise words (as standalone words)
    # But don't filter if the word is also a valid keyword (e.g., "lead" can be both)
    words = title_str.split()
    for noise in TITLE_NOISE:
        if noise in words and noise not in TITLE_ALLOW_KEYWORDS:
            return False
    
    # Too many words (job titles are typically 2-6 words, rarely more)
    if len(words) > 6:
        return False
    
    # Single word titles are usually not valid (except for very specific cases)
    if len(words) == 1:
        # Only allow single words if they're in the allowed keywords
        if words[0] not in TITLE_ALLOW_KEYWORDS:
            return False
    
    # Must contain at least one TECH-related keyword
    has_tech_keyword = any(keyword in title_str for keyword in TECH_KEYWORDS)
    if not has_tech_keyword:
        return False
    
    # Must NOT contain non-tech keywords (unless it's clearly tech-related)
    has_non_tech_keyword = any(keyword in title_str for keyword in NON_TECH_KEYWORDS)
    if has_non_tech_keyword:
        # Allow if it's clearly a tech role (e.g., "Sales Engineer" has both, but is tech)
        # But reject if it's clearly non-tech (e.g., "Store Manager", "Sales Associate")
        # Check if tech keyword appears before non-tech keyword or if it's a known tech role
        tech_roles_with_sales = ["sales engineer", "salesforce", "sales operations", "sales analytics", "salesforce engineer"]
        if any(tech_role in title_str for tech_role in tech_roles_with_sales):
            pass  # Allow these
        elif "store" in title_str or "retail" in title_str:
            return False  # Definitely not tech (e.g., "Store Manager", "Assistant Store Manager")
        elif "sales" in title_str and "engineer" not in title_str and "analyst" not in title_str and "salesforce" not in title_str:
            return False  # Sales roles without tech context
        elif "technician" in title_str:
            # Only allow if it's clearly tech (IT Technician, Network Technician, etc.)
            tech_technician_keywords = ["it", "network", "computer", "systems", "tech", "information", "software", "data"]
            if not any(tech_word in title_str for tech_word in tech_technician_keywords):
                return False  # Probably not a tech technician
    
    # Special handling for "manager" - only allow if it's clearly a tech manager role
    if "manager" in title_str:
        # Tech manager keywords that indicate it's a tech role
        tech_manager_keywords = ["engineering", "software", "product", "project", "program", "technical", 
                                "development", "devops", "data", "systems", "it", "information", "tech",
                                "platform", "infrastructure", "security", "cloud", "qa", "quality assurance",
                                "scrum", "agile", "release", "build", "ci/cd", "site reliability", "sre"]
        # Non-tech manager keywords that indicate it's NOT a tech role
        non_tech_manager_keywords = ["store", "retail", "sales", "marketing", "customer", "operations",
                                     "warehouse", "logistics", "restaurant", "food", "hotel", "hospitality"]
        
        # If it has non-tech manager keywords, reject
        if any(non_tech in title_str for non_tech in non_tech_manager_keywords):
            return False
        
        # If it's just "manager" or "assistant manager" without tech context, reject
        if not any(tech_mgr in title_str for tech_mgr in tech_manager_keywords):
            # But allow if it has other tech keywords
            if not has_tech_keyword:
                return False
    
    # Titles should start with a capital letter (proper formatting)
    if title_original and not title_original[0].isupper():
        # Allow if it's a valid keyword that happens to be lowercase
        first_word_lower = words[0] if words else ""
        if first_word_lower not in TITLE_ALLOW_KEYWORDS:
            return False
    
    # Additional checks for common invalid patterns
    # Reject if it looks like a sentence fragment
    if any(word in title_str for word in ["the", "a", "an", "and", "or", "with", "for", "to", "of", "in", "on", "at"]):
        # But allow if it's a proper title like "Manager of Engineering"
        if len(words) <= 4 and any(keyword in title_str for keyword in ["manager", "director", "lead", "head", "chief"]):
            pass  # Allow this pattern
        elif len(words) > 4:
            return False  # Too many words with common words = likely a sentence
    
    return True

def filter_valid_job_titles(df, title_column='title'):
    """Filter DataFrame to only include rows with valid job titles"""
    if df.empty or title_column not in df.columns:
        return df
    
    # Filter to valid titles
    mask = df[title_column].apply(is_valid_job_title)
    return df[mask].copy()

# Real data loading functions
@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_skills_demand():
    try:
        df = pd.read_parquet('data/gold/skills_demand.parquet')
        return df
    except Exception as e:
        st.error(f"Error loading skills demand: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_unified_salaries():
    try:
        # Try single parquet file first
        single_file = Path('data/silver/unified_salaries.parquet')
        if single_file.exists():
            df = pd.read_parquet(single_file)
            if not df.empty:
                return df
        
        # Try directory with parquet files
        dir_path = Path('data/silver/unified_salaries')
        if dir_path.exists() and dir_path.is_dir():
            files = list(dir_path.glob('*.parquet'))
            if files:
                dfs = [pd.read_parquet(f) for f in files]
                df = pd.concat(dfs, ignore_index=True)
                if not df.empty:
                    return df
        
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_job_postings():
    try:
        # Try to load from silver unified_postings first (cleaned data)
        unified_path = Path('data/silver/unified_postings.parquet')
        if unified_path.exists():
            df = pd.read_parquet(unified_path)
            # Map job_title to title for consistency
            if 'job_title' in df.columns and 'title' not in df.columns:
                df['title'] = df['job_title']
            # Filter to valid job titles
            df = filter_valid_job_titles(df, 'title')
            if not df.empty:
                return df
        
        # Fallback to bronze data with filtering
        files = list(Path('data/bronze/job_postings').glob('*.parquet'))[:3]  # Load first 3 files
        if files:
            dfs = [pd.read_parquet(f) for f in files]
            df = pd.concat(dfs, ignore_index=True)
            # Filter to valid job titles
            df = filter_valid_job_titles(df, 'title')
            return df
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_market_summary():
    try:
        salaries_df = load_unified_salaries()
        skills_df = load_skills_demand()
        jobs_df = load_job_postings()
        
        # Calculate average salary safely
        avg_salary = 0
        if not salaries_df.empty and 'salary_amount' in salaries_df.columns:
            numeric_salaries = safe_numeric_salary(salaries_df)
            avg_salary = float(numeric_salaries.mean()) if not numeric_salaries.isna().all() else 0
        
        # Get top skills safely
        top_skills = []
        if not skills_df.empty and 'skill' in skills_df.columns:
            top_skills = skills_df.head(5)['skill'].tolist()
        
        # Get trending roles safely
        trending_roles = []
        if not jobs_df.empty and 'title' in jobs_df.columns:
            trending_roles = jobs_df['title'].value_counts().head(5).index.tolist()
        
        summary = {
            "total_jobs": len(jobs_df) if not jobs_df.empty else 0,
            "avg_salary": avg_salary,
            "top_skills": top_skills,
            "trending_roles": trending_roles
        }
        return summary
    except Exception as e:
        return {
            "total_jobs": 0,
            "avg_salary": 0,
            "top_skills": [],
            "trending_roles": []
        }

# Main app
def main():
    st.title("🎯 Smart Career Planner")
    st.markdown("Get personalized skill recommendations based on your current skills and target role.")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", [
        "Career Planner", 
        "Market Overview", 
        "Skill Analysis",
        "Salary Intelligence", 
        "Real-time Trends",
        "About"
    ])
    
    if page == "Career Planner":
        show_career_planner()
    elif page == "Market Overview":
        show_market_overview()
    elif page == "Skill Analysis":
        show_skill_analysis()
    elif page == "Salary Intelligence":
        show_salary_intelligence()
    elif page == "Real-time Trends":
        show_realtime_trends()
    else:
        show_about()

def show_career_planner():
    st.header("🎯 Personalized Career Recommendations")
    
    # Input form
    col1, col2 = st.columns(2)
    
    with col1:
        current_skills = st.text_input(
            "Your current skills (comma-separated)",
            value="swift, ios frameworks",
            help="Enter your current skills separated by commas"
        )
    
    with col2:
        target_role = st.text_input(
            "Target role (optional)",
            value="iOS Engineer",
            help="What role are you targeting?"
        )
    
    # Get recommendations button
    if st.button("🚀 Get Smart Recommendations", type="primary", use_container_width=True):
        if not current_skills.strip():
            st.warning("Please enter your current skills")
            return
        
        with st.spinner("🤖 Analyzing your skills and generating personalized recommendations..."):
            skills_list = [s.strip().lower() for s in current_skills.split(",") if s.strip()]
            result = call_career_api(skills_list, target_role)
            
            if result.get("error"):
                st.error(f"Error: {result['error']}")
                return
            
            # Display results
            display_recommendations(result)

def display_recommendations(data):
    st.success(f"🎉 Found {len(data.get('recommended_skills', []))} personalized recommendations!")
    
    # Categorized recommendations
    categorized = data.get("categorized_recommendations", {})
    
    if categorized:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("🔥 Must Learn")
            st.markdown("*Critical for your target role*")
            must_learn = categorized.get("must_learn", [])
            if must_learn:
                for skill in must_learn:
                    st.write(f"• **{skill.title()}**")
            else:
                st.info("No must-have skills identified")
        
        with col2:
            st.subheader("📈 Should Learn")
            st.markdown("*Important for career growth*")
            should_learn = categorized.get("should_learn", [])
            if should_learn:
                for skill in should_learn[:5]:  # Show top 5
                    st.write(f"• **{skill.title()}**")
            else:
                st.info("No should-have skills identified")
        
        with col3:
            st.subheader("✨ Nice to Have")
            st.markdown("*Future career opportunities*")
            nice_to_have = categorized.get("nice_to_have", [])
            if nice_to_have:
                for skill in nice_to_have[:5]:  # Show top 5
                    st.write(f"• **{skill.title()}**")
            else:
                st.info("No nice-to-have skills identified")
    
    # Learning path
    learning_path = data.get("learning_path", {})
    if learning_path:
        st.markdown("---")
        st.subheader("🎯 Your Learning Path")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Phase 1: Immediate Focus**")
            immediate = learning_path.get("immediate_focus", [])
            for skill in immediate:
                st.write(f"• {skill.title()}")
        
        with col2:
            st.markdown("**Phase 2: Next Phase**")
            next_phase = learning_path.get("next_phase", [])
            for skill in next_phase:
                st.write(f"• {skill.title()}")
        
        with col3:
            st.markdown("**Phase 3: Future Goals**")
            future = learning_path.get("future_goals", [])
            for skill in future:
                st.write(f"• {skill.title()}")
    
    # Summary metrics
    st.markdown("---")
    st.subheader("📊 Analysis Summary")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Skills Analyzed", f"{data.get('total_skills_analyzed', 0):,}")
    with col2:
        st.metric("Your Current Skills", len(data.get('your_skills', [])))
    with col3:
        st.metric("Recommendations", len(data.get('recommended_skills', [])))

def show_market_overview():
    st.header("📊 Job Market Overview")
    
    # Load real data
    with st.spinner("Loading real market data..."):
        data = load_market_summary()
        skills_df = load_skills_demand()
        salaries_df = load_unified_salaries()
        jobs_df = load_job_postings()
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Jobs", f"{data['total_jobs']:,}")
    with col2:
        st.metric("Average Salary", f"${data['avg_salary']:,.0f}")
    with col3:
        st.metric("Top Skills", len(data['top_skills']))
    with col4:
        st.metric("Trending Roles", len(data['trending_roles']))
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Skills (Real Data)")
        if not skills_df.empty:
            top_skills = skills_df.head(10)
            fig = px.bar(top_skills, x='skill', y='count', title="Most In-Demand Skills", 
                        color='count', color_continuous_scale='viridis')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No skills data available")
    
    with col2:
        st.subheader("Trending Roles (Real Data)")
        if not jobs_df.empty:
            top_roles = jobs_df['title'].value_counts().head(10).reset_index()
            top_roles.columns = ['Role', 'Count']
            fig = px.bar(top_roles, x='Role', y='Count', title="Most Posted Job Titles",
                        color='Count', color_continuous_scale='plasma')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No job postings data available")
    
    # Additional real data insights
    if not salaries_df.empty and 'currency' in salaries_df.columns and 'period' in salaries_df.columns:
        st.subheader("Salary Distribution (Real Data)")
        col1, col2 = st.columns(2)
        
        with col1:
            # Salary by period (more meaningful than currency)
            period_counts = salaries_df['period'].value_counts()
            if not period_counts.empty:
                fig = px.pie(values=period_counts.values, names=period_counts.index, 
                            title="Salary Distribution by Pay Period",
                            color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No period data available")
        
        with col2:
            # Average salary by period (more informative)
            if 'salary_amount' in salaries_df.columns:
                numeric_salaries = safe_numeric_salary(salaries_df)
                if not numeric_salaries.isna().all():
                    salaries_df_copy = salaries_df.copy()
                    salaries_df_copy['salary_amount_numeric'] = numeric_salaries
                    # Filter out invalid salaries
                    valid_salaries = salaries_df_copy[salaries_df_copy['salary_amount_numeric'].notna() & 
                                                     (salaries_df_copy['salary_amount_numeric'] > 0)]
                    
                    if not valid_salaries.empty and 'period' in valid_salaries.columns:
                        # Calculate average salary by period
                        avg_by_period = valid_salaries.groupby('period')['salary_amount_numeric'].mean().sort_values(ascending=False)
                        if not avg_by_period.empty:
                            fig = px.bar(x=avg_by_period.index, y=avg_by_period.values, 
                                        title="Average Salary by Pay Period",
                                        labels={'x': 'Pay Period', 'y': 'Average Salary (USD)'},
                                        color=avg_by_period.values,
                                        color_continuous_scale='viridis')
                            fig.update_layout(showlegend=False)
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("No valid salary data by period")
                    else:
                        st.info("No valid salary data available")
                else:
                    st.info("No numeric salary data available")
            else:
                # Fallback to period counts
                period_counts = salaries_df['period'].value_counts()
                if not period_counts.empty:
                    fig = px.bar(x=period_counts.index, y=period_counts.values, 
                                title="Job Count by Pay Period")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No period data available")

def show_skill_analysis():
    st.header("📊 Skill Analysis")
    
    # Load real skills data
    with st.spinner("Loading real skills data..."):
        skills_df = load_skills_demand()
    
    if skills_df.empty:
        st.warning("No skills data available. Please run the ETL pipeline first.")
        return
    
    # Skill demand chart
    st.subheader("Top In-Demand Skills (Real Data)")
    top_skills = skills_df.head(15)
    fig = px.bar(top_skills, x='skill', y='count', title="Most In-Demand Skills", 
                color='count', color_continuous_scale='viridis')
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Skill analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Skills by Category (Real Data)")
        if 'category' in skills_df.columns:
            category_counts = skills_df['category'].value_counts()
            fig = px.pie(values=category_counts.values, names=category_counts.index, 
                        title="Skills Distribution by Category")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Category information not available")
    
    with col2:
        st.subheader("Top Skills Table")
        if not skills_df.empty and all(col in skills_df.columns for col in ['skill', 'count', 'category']):
            st.dataframe(
                skills_df[['skill', 'count', 'category']].head(10),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Skills table data not available")
    
    # Skill insights
    st.subheader("📈 Skill Insights")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Skills", len(skills_df))
    with col2:
        st.metric("Total Demand", f"{skills_df['count'].sum():,}")
    with col3:
        st.metric("Avg Demand", f"{skills_df['count'].mean():.0f}")
    
    # Skill categories breakdown
    if 'category' in skills_df.columns:
        st.subheader("Skills by Category")
        category_skills = skills_df.groupby('category').agg({
            'skill': 'count',
            'count': 'sum'
        }).reset_index()
        category_skills.columns = ['Category', 'Skill Count', 'Total Demand']
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(category_skills, x='Category', y='Skill Count', 
                        title="Number of Skills by Category")
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(category_skills, x='Category', y='Total Demand', 
                        title="Total Demand by Category")
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)

def show_salary_intelligence():
    st.header("💰 Salary Intelligence")
    
    # Load real salary data
    with st.spinner("Loading real salary data..."):
        salaries_df = load_unified_salaries()
        jobs_df = load_job_postings()
    
    if salaries_df.empty or 'salary_amount' not in salaries_df.columns:
        st.warning("No salary data available. Please run the ETL pipeline first.")
        return
    
    # Normalize all salaries to yearly USD for proper comparison
    salaries_normalized = normalize_salaries_to_yearly_usd(salaries_df)
    
    if salaries_normalized.empty:
        st.warning("No valid salary data after normalization. Please check the data.")
        return
    
    # Salary calculator
    st.subheader("Salary Calculator (Based on Real Data)")
    st.info("💡 All salaries are normalized to yearly USD for accurate comparison across currencies and pay periods.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        years_exp = st.slider("Years of Experience", 0, 20, 5)
    with col2:
        output_period = st.selectbox("Display Salary As", ["YEARLY", "MONTHLY", "WEEKLY", "HOURLY"], index=0)
    
    if st.button("Calculate Salary", type="primary"):
        with st.spinner("Calculating salary based on real data..."):
            # Use normalized yearly USD salaries
            if 'salary_yearly_usd' in salaries_normalized.columns:
                base_salary_yearly = float(salaries_normalized['salary_yearly_usd'].mean())
                
                # Experience adjustment (10% per year, capped at 100% increase)
                exp_multiplier = min(1 + (years_exp * 0.1), 2.0)
                adjusted_salary_yearly = base_salary_yearly * exp_multiplier
                
                # Convert to requested period
                if output_period == "YEARLY":
                    display_salary = adjusted_salary_yearly
                    period_label = "per year"
                elif output_period == "MONTHLY":
                    display_salary = adjusted_salary_yearly / 12
                    period_label = "per month"
                elif output_period == "WEEKLY":
                    display_salary = adjusted_salary_yearly / 52
                    period_label = "per week"
                elif output_period == "HOURLY":
                    display_salary = adjusted_salary_yearly / 2080
                    period_label = "per hour"
                else:
                    display_salary = adjusted_salary_yearly
                    period_label = "per year"
                
                st.success(f"💰 Estimated Salary: **${display_salary:,.2f} {period_label}**")
                st.info(f"📊 Based on {len(salaries_normalized):,} real salary records (normalized to yearly USD)")
                
                # Show breakdown
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Base Salary (0 years)", f"${base_salary_yearly:,.0f}/year")
                with col2:
                    st.metric("Your Experience", f"{years_exp} years")
                with col3:
                    st.metric("Adjusted Salary", f"${adjusted_salary_yearly:,.0f}/year")
            else:
                st.warning("Unable to calculate salary. Data normalization failed.")
    
    # Real salary distributions (using normalized yearly USD)
    st.subheader("Salary Distributions (Normalized to Yearly USD)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Salary distribution histogram
        if 'salary_yearly_usd' in salaries_normalized.columns:
            fig = px.histogram(salaries_normalized, x='salary_yearly_usd', nbins=50, 
                             title="Salary Distribution (Yearly USD)",
                             labels={'salary_yearly_usd': 'Salary (USD/year)', 'count': 'Number of Jobs'},
                             color_discrete_sequence=['#1f77b4'])
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No normalized salary data available")
    
    with col2:
        # Salary by original period (showing normalized values)
        if 'period' in salaries_normalized.columns and 'salary_yearly_usd' in salaries_normalized.columns:
            period_stats = salaries_normalized.groupby('period')['salary_yearly_usd'].agg(['mean', 'median', 'count']).reset_index()
            period_stats.columns = ['Period', 'Mean (USD/year)', 'Median (USD/year)', 'Count']
            period_stats = period_stats.sort_values('Mean (USD/year)', ascending=False)
            
            fig = px.bar(period_stats, x='Period', y='Mean (USD/year)', 
                         title="Average Salary by Original Pay Period\n(All normalized to yearly USD)",
                         labels={'Mean (USD/year)': 'Average Salary (USD/year)'},
                         color='Mean (USD/year)',
                         color_continuous_scale='viridis')
            st.plotly_chart(fig, use_container_width=True)
            
            # Show the table
            st.caption("Salary statistics by original pay period:")
            st.dataframe(period_stats[['Period', 'Mean (USD/year)', 'Median (USD/year)', 'Count']], 
                        use_container_width=True, hide_index=True)
        else:
            st.info("Period data not available")
    
    # Salary insights (using normalized data)
    st.subheader("📊 Salary Insights (All normalized to Yearly USD)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", f"{len(salaries_normalized):,}")
    with col2:
        if 'salary_yearly_usd' in salaries_normalized.columns:
            avg_sal = float(salaries_normalized['salary_yearly_usd'].mean())
        else:
            avg_sal = 0
        st.metric("Average Salary", f"${avg_sal:,.0f}/year")
    with col3:
        if 'salary_yearly_usd' in salaries_normalized.columns:
            median_sal = float(salaries_normalized['salary_yearly_usd'].median())
        else:
            median_sal = 0
        st.metric("Median Salary", f"${median_sal:,.0f}/year")
    with col4:
        if 'salary_yearly_usd' in salaries_normalized.columns:
            p75_sal = float(salaries_normalized['salary_yearly_usd'].quantile(0.75))
        else:
            p75_sal = 0
        st.metric("75th Percentile", f"${p75_sal:,.0f}/year")
    
    # Salary quartiles and percentiles
    st.subheader("Salary Percentiles (Yearly USD)")
    if 'salary_yearly_usd' in salaries_normalized.columns:
        percentiles = [10, 25, 50, 75, 90, 95]
        percentile_values = [float(salaries_normalized['salary_yearly_usd'].quantile(p/100)) for p in percentiles]
        percentile_df = pd.DataFrame({
            'Percentile': [f'{p}th' for p in percentiles],
            'Salary (USD/year)': percentile_values
        })
        
        fig = px.bar(percentile_df, x='Percentile', y='Salary (USD/year)',
                     title="Salary Distribution Percentiles",
                     color='Salary (USD/year)',
                     color_continuous_scale='viridis')
        st.plotly_chart(fig, use_container_width=True)
        
        # Show table
        st.dataframe(percentile_df, use_container_width=True, hide_index=True)

def show_realtime_trends():
    st.header("📈 Real-time Trends")
    
    # Load real data
    with st.spinner("Loading real market trends..."):
        jobs_df = load_job_postings()
        skills_df = load_skills_demand()
        salaries_df = load_unified_salaries()
    
    if jobs_df.empty:
        st.warning("No job postings data available. Please run the ETL pipeline first.")
        return
    
    # Trending jobs (real data)
    st.subheader("🔥 Trending Job Titles (Real Data)")
    
    # Get top job titles
    top_jobs = jobs_df['title'].value_counts().head(10).reset_index()
    top_jobs.columns = ['Job Title', 'Postings']
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(top_jobs, x='Job Title', y='Postings', title="Most Posted Job Titles",
                    color='Postings', color_continuous_scale='viridis')
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Show job titles table
        st.subheader("Top Job Titles")
        if not top_jobs.empty:
            st.dataframe(top_jobs, use_container_width=True, hide_index=True)
        else:
            st.info("No job titles data available")
    
    # Trending skills (real data)
    st.subheader("🚀 Trending Skills (Real Data)")
    
    if not skills_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Skills demand scatter
            fig = px.scatter(skills_df.head(15), x='skill', y='count', size='count', 
                            hover_name='skill', title="Skill Demand Distribution",
                            color='count', color_continuous_scale='plasma')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Skills by category
            if 'category' in skills_df.columns:
                category_counts = skills_df['category'].value_counts()
                fig = px.pie(values=category_counts.values, names=category_counts.index, 
                            title="Skills by Category")
                st.plotly_chart(fig, use_container_width=True)
    
    # Market insights (real data)
    st.subheader("📊 Market Insights (Real Data)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Job Postings", f"{len(jobs_df):,}")
    with col2:
        if not salaries_df.empty and 'salary_amount' in salaries_df.columns:
            numeric_salaries = safe_numeric_salary(salaries_df)
            avg_salary = float(numeric_salaries.mean()) if not numeric_salaries.isna().all() else 0
        else:
            avg_salary = 0
        st.metric("Average Salary", f"${avg_salary:,.0f}")
    with col3:
        unique_companies = jobs_df['company_name'].nunique() if not jobs_df.empty and 'company_name' in jobs_df.columns else 0
        st.metric("Unique Companies", f"{unique_companies:,}")
    with col4:
        st.metric("Skills Tracked", f"{len(skills_df):,}")
    
    # Company insights
    st.subheader("🏢 Company Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top companies by job postings
        if 'company_name' in jobs_df.columns:
            top_companies = jobs_df['company_name'].value_counts().head(10).reset_index()
            top_companies.columns = ['Company', 'Job Postings']
            
            fig = px.bar(top_companies, x='Company', y='Job Postings', 
                         title="Top Companies by Job Postings",
                         color='Job Postings', color_continuous_scale='blues')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Company name data not available")
    
    with col2:
        # Job posting distribution
        if 'max_salary' in jobs_df.columns:
            # Filter out invalid salaries
            valid_salaries = jobs_df[jobs_df['max_salary'].notna() & (jobs_df['max_salary'] > 0)]
            if not valid_salaries.empty:
                fig = px.histogram(valid_salaries, x='max_salary', nbins=30, 
                                 title="Salary Distribution in Job Postings")
                st.plotly_chart(fig, use_container_width=True)
    
    # Recent trends analysis
    st.subheader("📈 Trend Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Skills demand ranking
        if not skills_df.empty and all(col in skills_df.columns for col in ['skill', 'count', 'category']):
            st.subheader("Top 10 Skills by Demand")
            top_skills_display = skills_df.head(10)[['skill', 'count', 'category']]
            st.dataframe(top_skills_display, use_container_width=True, hide_index=True)
        else:
            st.info("Skills ranking data not available")
    
    with col2:
        # Job title word cloud (simplified)
        st.subheader("Most Common Job Title Words")
        if not jobs_df.empty:
            # Extract common words from job titles
            all_titles = ' '.join(jobs_df['title'].dropna().astype(str))
            words = all_titles.lower().split()
            # Filter out common words
            common_words = ['engineer', 'developer', 'analyst', 'manager', 'specialist', 'senior', 'junior']
            word_counts = {}
            for word in words:
                if len(word) > 3 and word in common_words:
                    word_counts[word] = word_counts.get(word, 0) + 1
            
            if word_counts:
                word_df = pd.DataFrame(list(word_counts.items()), columns=['Word', 'Count'])
                word_df = word_df.sort_values('Count', ascending=False)
                
                fig = px.bar(word_df, x='Word', y='Count', title="Most Common Job Title Words")
                st.plotly_chart(fig, use_container_width=True)

def show_about():
    st.header("📚 About Job Market Analysis Platform")
    
    st.markdown("""
    ## 🎯 Project Overview
    
    The **Job Market Analysis Platform** is a comprehensive Big Data analytics solution designed to provide 
    real-time insights into the technology job market. This platform processes and analyzes massive datasets 
    from multiple sources to deliver actionable career intelligence, salary predictions, and skill demand 
    forecasting.
    
    ### 📊 Scale & Impact
    
    - **129.68 GB** of data processed (6.5x the original 20GB+ requirement)
    - **2.7+ million records** across 4 major data sources
    - **1,488 GitHub Archive files** containing real-time developer activity
    - **514,000+ StackOverflow survey responses** from 2019-2025
    - **2.2+ million job postings** from Kaggle datasets
    - **Real-time data processing** with distributed computing capabilities
    
    ## 🏗️ Architecture & Technology
    
    ### Data Lake Architecture
    This platform implements a modern 3-layer data lake architecture:
    - **Raw Layer**: Original data from multiple sources (126.54 GB)
    - **Bronze Layer**: Cleaned and standardized data (3.14 GB)
    - **Silver Layer**: Unified datasets across sources
    - **Gold Layer**: ML-ready features and aggregations
    
    ### Big Data Tools & Technologies
    - **Apache Spark**: Distributed data processing for large-scale analytics
    - **Delta Lake**: Versioned data storage with ACID transactions
    - **Apache Airflow**: Workflow orchestration for automated pipelines
    - **Apache Kafka**: Real-time streaming data processing
    - **MLflow**: Machine learning experiment tracking and model management
    - **XGBoost**: Advanced machine learning for salary prediction
    - **FastAPI**: High-performance REST API for data access
    - **Streamlit**: Interactive dashboard for data visualization
    
    ## 🚀 Key Features
    
    ### 💼 Career Intelligence
    - **Personalized Skill Recommendations**: AI-powered suggestions based on your current skills and target role
    - **Role-Specific Learning Paths**: Structured progression from immediate focus to future goals
    - **Categorized Learning**: Must Learn, Should Learn, and Nice to Have skills
    - **Dynamic Recommendations**: Different suggestions for iOS, Data Science, Web Development, and DevOps roles
    
    ### 📈 Market Analytics
    - **Real-time Job Market Trends**: Live insights into trending job titles and skills
    - **Salary Intelligence**: Comprehensive salary analysis with currency conversion and pay period normalization
    - **Skill Demand Analysis**: In-depth analysis of in-demand skills and categories
    - **Market Overview**: KPIs, trends, and growth metrics
    
    ### 🤖 Machine Learning
    - **Salary Prediction**: XGBoost-based models for accurate salary estimation
    - **Skill Forecasting**: Predictive analytics for emerging skill trends
    - **Experience-based Adjustments**: Intelligent salary calculations based on years of experience
    
    ## 👥 Development Team
    
    This project was developed by a dedicated team of data engineers, developers, and machine learning specialists:
    
    ### 🚀 **Manikanta** - Project Lead & Data Engineering
    - **Responsibilities**: Environment setup, Apache Spark ETL pipeline development, Apache Airflow orchestration, Delta Lake integration
    - **Achievements**: Built distributed ETL pipeline processing 129.68 GB of data, automated workflow orchestration, implemented data lake architecture
    
    ### 📊 **Sheila** - Data Collection & Preprocessing
    - **Responsibilities**: Data ingestion from multiple sources, data quality assurance, preprocessing pipelines
    - **Achievements**: Collected and processed data from GitHub Archive, StackOverflow, Kaggle, and BLS sources
    
    ### 🌐 **Deepti** - API & Dashboard Development
    - **Responsibilities**: FastAPI service development, Streamlit dashboard creation, user interface design
    - **Achievements**: Built Smart Career API with intelligent recommendations, developed interactive 6-tab dashboard with real-time visualizations
    
    ### 🤖 **Rahul** - Machine Learning & Model Tracking
    - **Responsibilities**: ML model development, XGBoost implementation, MLflow integration, model evaluation
    - **Achievements**: Developed salary prediction models, implemented skill forecasting, set up ML experiment tracking
    
    ## 📚 Data Sources
    
    - **GitHub Archive**: Real-time developer activity and repository data (123.43 GB)
    - **StackOverflow Developer Surveys**: Annual surveys from 2019-2025 (514K+ responses)
    - **Kaggle Job Market Data**: Comprehensive job postings and salary information (2.2M+ records)
    - **BLS Employment Data**: Official employment statistics and trends
    
    ## 🎓 Academic Project
    
    This platform was developed as part of a Big Data Analytics course project, demonstrating:
    - **Big Data Characteristics**: Volume (129GB), Variety (4 sources), Velocity (real-time + batch), Veracity (quality assurance)
    - **Distributed Computing**: Apache Spark for large-scale processing
    - **Modern Data Architecture**: Data lake implementation with versioning
    - **ML at Scale**: Production-ready machine learning pipelines
    - **Real-time Analytics**: Live dashboards and APIs
    
    ## 🔧 Technical Highlights
    
    - **Performance**: <100ms API response times, <1 second dashboard load times
    - **Scalability**: Distributed processing capable of handling 100+ GB datasets
    - **Reliability**: Automated data quality validation and error handling
    - **User Experience**: Beautiful, responsive UI with interactive visualizations
    - **Data Quality**: Comprehensive filtering and normalization for accurate insights
    
    ## 📞 Contact & Support
    
    For questions, issues, or contributions, please refer to the project repository or contact the development team.
    """)
    
    st.success("🎉 **Thank you for using the Job Market Analysis Platform!** We hope this tool helps you make informed career decisions.")

if __name__ == "__main__":
    main()
