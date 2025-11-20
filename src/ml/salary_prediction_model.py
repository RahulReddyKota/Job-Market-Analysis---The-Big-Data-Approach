#!/usr/bin/env python3
"""
ML Pipeline for Salary Prediction using XGBoost and MLflow
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
import mlflow
import mlflow.xgboost
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb
import joblib

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.common.logs import get_logger

logger = get_logger("ml.salary_prediction")

class SalaryPredictionModel:
    """XGBoost model for salary prediction with MLflow tracking"""
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_columns = []
        
        # Set up MLflow
        mlflow.set_experiment("job_market_salary_prediction")
    
    def load_data(self, data_path="data/silver/unified_salaries.parquet"):
        """Load salary data for training"""
        logger.info(f"Loading salary data from {data_path}")
        
        try:
            path = Path(data_path)
            if path.is_file():
                df = pd.read_parquet(path)
            else:
                # Try directory form (Spark wrote a folder of Parquet parts)
                dir_path = Path("data/silver/unified_salaries")
                if dir_path.exists() and dir_path.is_dir():
                    df = pd.read_parquet(dir_path)
                else:
                    raise FileNotFoundError(f"Not found: {path} or {dir_path}")
            logger.info(f"Loaded {len(df)} salary records")
            return df
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return None
    
    def prepare_features(self, df):
        """Prepare features for ML model"""
        logger.info("Preparing features for ML model...")
        
        # Clean and prepare data
        df_clean = df.copy()
        
        # Handle missing values
        # Coerce salary to numeric
        if 'salary_amount' in df_clean.columns:
            df_clean['salary_amount'] = pd.to_numeric(df_clean['salary_amount'], errors='coerce')
        df_clean = df_clean.dropna(subset=['salary_amount'])
        
        # Filter reasonable salary ranges
        df_clean = df_clean[
            (df_clean['salary_amount'] > 1000) & 
            (df_clean['salary_amount'] < 1000000)
        ]
        
        # Create features
        features = []
        
        # Currency encoding
        if 'currency' in df_clean.columns:
            le_currency = LabelEncoder()
            df_clean['currency_encoded'] = le_currency.fit_transform(df_clean['currency'].fillna('USD'))
            self.label_encoders['currency'] = le_currency
            features.append('currency_encoded')
        
        # Period encoding
        if 'period' in df_clean.columns:
            le_period = LabelEncoder()
            df_clean['period_encoded'] = le_period.fit_transform(df_clean['period'].fillna('YEARLY'))
            self.label_encoders['period'] = le_period
            features.append('period_encoded')
        
        # Add more features if available
        if 'job_id' in df_clean.columns:
            # Create job_id length as a feature
            df_clean['job_id_length'] = df_clean['job_id'].astype(str).str.len()
            features.append('job_id_length')
        
        # Target variable
        target = 'salary_amount'
        
        # Prepare feature matrix
        X = df_clean[features].fillna(0)
        y = df_clean[target]
        
        self.feature_columns = features
        
        logger.info(f"Prepared {len(features)} features for {len(X)} samples")
        return X, y
    
    def train_model(self, X, y, test_size=0.2, random_state=42):
        """Train XGBoost model with MLflow tracking"""
        logger.info("Training XGBoost salary prediction model...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Start MLflow run
        with mlflow.start_run():
            # Log parameters
            params = {
                'test_size': test_size,
                'random_state': random_state,
                'n_estimators': 100,
                'max_depth': 6,
                'learning_rate': 0.1,
                'subsample': 0.8
            }
            mlflow.log_params(params)
            
            # Train XGBoost model
            self.model = xgb.XGBRegressor(
                n_estimators=params['n_estimators'],
                max_depth=params['max_depth'],
                learning_rate=params['learning_rate'],
                subsample=params['subsample'],
                random_state=random_state
            )
            
            self.model.fit(X_train_scaled, y_train)
            
            # Make predictions
            y_pred_train = self.model.predict(X_train_scaled)
            y_pred_test = self.model.predict(X_test_scaled)
            
            # Calculate metrics
            train_mae = mean_absolute_error(y_train, y_pred_train)
            test_mae = mean_absolute_error(y_test, y_pred_test)
            train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
            test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
            train_r2 = r2_score(y_train, y_pred_train)
            test_r2 = r2_score(y_test, y_pred_test)
            
            # Log metrics
            metrics = {
                'train_mae': train_mae,
                'test_mae': test_mae,
                'train_rmse': train_rmse,
                'test_rmse': test_rmse,
                'train_r2': train_r2,
                'test_r2': test_r2
            }
            mlflow.log_metrics(metrics)
            
            # Log model
            mlflow.xgboost.log_model(self.model, "salary_prediction_model")
            
            # Log feature importance
            feature_importance = dict(zip(self.feature_columns, self.model.feature_importances_))
            mlflow.log_params(feature_importance)
            
            logger.info("Model training complete!")
            logger.info(f"Test MAE: ${test_mae:,.2f}")
            logger.info(f"Test RMSE: ${test_rmse:,.2f}")
            logger.info(f"Test R²: {test_r2:.3f}")
            
            return metrics
    
    def save_model(self, model_path="ml/models/salary_model.pkl"):
        """Save trained model and preprocessors in API-compatible format"""
        logger.info(f"Saving model to {model_path}")
        
        # Create models directory
        Path(model_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Create API-compatible model format
        model_data = {
            'status': 'trained',
            'mean_model': self.model,
            'lower_model': self.model,  # Use same model for simplicity
            'upper_model': self.model,  # Use same model for simplicity
            'scaler': self.scaler,
            'label_encoders': self.label_encoders,
            'feature_columns': self.feature_columns,
            'metrics': {
                'test_mae': 0,  # Will be updated after training
                'test_r2': 0
            }
        }
        
        joblib.dump(model_data, model_path)
        logger.info("Model saved successfully")
    
    def load_model(self, model_path="models/salary_prediction_model.pkl"):
        """Load trained model and preprocessors"""
        logger.info(f"Loading model from {model_path}")
        
        try:
            model_data = joblib.load(model_path)
            # Handle both old format ('model') and new format ('mean_model')
            self.model = model_data.get('mean_model') or model_data.get('model')
            self.scaler = model_data['scaler']
            self.label_encoders = model_data['label_encoders']
            self.feature_columns = model_data['feature_columns']
            logger.info("Model loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            return False
    
    def predict_salary(self, features_dict):
        """Predict salary for given features"""
        if self.model is None:
            logger.error("Model not loaded. Please train or load a model first.")
            return None
        
        try:
            # Prepare features
            features = []
            for col in self.feature_columns:
                if col in features_dict:
                    features.append(features_dict[col])
                else:
                    features.append(0)
            
            # Scale features
            features_scaled = self.scaler.transform([features])
            
            # Make prediction
            prediction = self.model.predict(features_scaled)[0]
            
            logger.info(f"Predicted salary: ${prediction:,.2f}")
            return prediction
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return None

def main():
    """Main function to train salary prediction model"""
    logger.info("Starting Salary Prediction Model Training")
    
    # Initialize model
    model = SalaryPredictionModel()
    
    # Load data
    df = model.load_data()
    if df is None:
        return
    
    # Prepare features
    X, y = model.prepare_features(df)
    if len(X) == 0:
        logger.error("No features prepared. Exiting.")
        return
    
    # Train model
    metrics = model.train_model(X, y)
    
    # Save model
    model.save_model()
    
    # Example prediction
    example_features = {
        'currency_encoded': 0,  # USD
        'period_encoded': 0,    # YEARLY
        'job_id_length': 10
    }
    
    prediction = model.predict_salary(example_features)
    
    logger.info("Salary prediction model training complete!")

if __name__ == "__main__":
    main()
