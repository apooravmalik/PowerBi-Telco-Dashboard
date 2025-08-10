import pandas as pd
import logging
from datetime import datetime
from sqlalchemy import text, inspect
from database import engine, DB_SCHEMA, test_connection_CustomerPredictions_db

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_and_validate_csv(file_path='combined_model_predictions.csv'):
    """Load and validate the CSV file"""
    try:
        logger.info(f"Loading data from {file_path}...")
        df = pd.read_csv(file_path)
        
        # Validate required columns
        required_columns = [
            'CustomerID', 'LR_Prediction', 'LR_Probability', 'Actual_Churn',
            'Risk_Category', 'XGB_Prediction', 'XGB_Probability', 'XGB_Risk_Category'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Data validation and type conversion
        logger.info("Validating and cleaning data...")
        
        # Convert to proper data types
        df['LR_Prediction'] = df['LR_Prediction'].astype(int)
        df['XGB_Prediction'] = df['XGB_Prediction'].astype(int)
        df['Actual_Churn'] = df['Actual_Churn'].astype(int)
        df['LR_Probability'] = pd.to_numeric(df['LR_Probability'], errors='coerce')
        df['XGB_Probability'] = pd.to_numeric(df['XGB_Probability'], errors='coerce')
        
        # Remove any rows with invalid data
        initial_count = len(df)
        df = df.dropna(subset=required_columns)
        final_count = len(df)
        
        if initial_count != final_count:
            logger.warning(f"Removed {initial_count - final_count} rows with missing/invalid data")
        
        # Validate ranges
        prob_issues = ((df['LR_Probability'] < 0) | (df['LR_Probability'] > 1) | 
                      (df['XGB_Probability'] < 0) | (df['XGB_Probability'] > 1)).sum()
        if prob_issues > 0:
            logger.warning(f"Found {prob_issues} rows with probability values outside 0-1 range")
        
        logger.info(f"✅ Data validation completed. Final dataset: {len(df)} records")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"LR Risk Category distribution:\n{df['Risk_Category'].value_counts()}")
        logger.info(f"XGB Risk Category distribution:\n{df['XGB_Risk_Category'].value_counts()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading/validating CSV: {e}")
        raise
    
    
def insert_data_to_db(df, batch_size=1000):
    """Insert data to CustomerPredictions table in batches"""
    try:
        logger.info(f"Inserting {len(df)} records to database...")
        
        # Add timestamp columns
        current_time = datetime.now()
        df_insert = df.copy()
        df_insert['CreatedDate'] = current_time
        df_insert['LastUpdated'] = current_time
        
        # Select only the columns that match the database schema
        db_columns = [
            'CustomerID', 'LR_Prediction', 'LR_Probability', 'Actual_Churn',
            'Risk_Category', 'XGB_Prediction', 'XGB_Probability', 'XGB_Risk_Category',
            'CreatedDate', 'LastUpdated'
        ]
        
        df_final = df_insert[db_columns]
        
        # Insert data in batches to handle large datasets
        total_inserted = 0
        errors = 0
        
        for i in range(0, len(df_final), batch_size):
            batch = df_final.iloc[i:i+batch_size]
            
            try:
                batch.to_sql(
                    'CustomerPredictions',
                    engine,
                    schema=DB_SCHEMA,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_inserted += len(batch)
                logger.info(f"✅ Inserted batch: {total_inserted}/{len(df_final)} records")
                
            except Exception as batch_error:
                errors += len(batch)
                logger.error(f"❌ Error inserting batch {i//batch_size + 1}: {batch_error}")
                
                # Try inserting records one by one to identify problematic rows
                for idx, row in batch.iterrows():
                    try:
                        pd.DataFrame([row]).to_sql(
                            'CustomerPredictions',
                            engine,
                            schema=DB_SCHEMA,
                            if_exists='append',
                            index=False
                        )
                        total_inserted += 1
                    except Exception as row_error:
                        logger.error(f"❌ Error inserting CustomerID {row['CustomerID']}: {row_error}")
        
        logger.info(f"✅ Data insertion completed!")
        logger.info(f"   Successfully inserted: {total_inserted} records")
        if errors > 0:
            logger.warning(f"   Failed to insert: {errors} records")
        
        return total_inserted
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise
    
def main():
    """Main execution function"""
    try:
        print("\n🚀 CUSTOMER CHURN PREDICTIONS - DATA LOADER")
        print("="*50)
        
        # Step 1: Test connection using your existing database.py
        logger.info("Step 1: Testing database connection...")
        test_connection_CustomerPredictions_db()
        
        
        # Step 2: Load and validate CSV
        logger.info("Step 3: Loading CSV data...")
        csv_file = 'combined_model_predictions.csv'
            
        
        df = load_and_validate_csv(csv_file)
        
        # Step 3: Insert data
        logger.info("Step 5: Inserting data to database...")
        batch_size = 1000  # Adjust batch size as needed
        total_inserted = insert_data_to_db(df, batch_size=batch_size)
        logger.info(f"✅ Process completed successfully! Total records inserted: {total_inserted}")
        
        return True
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        return False
    except Exception as e:
        logger.error(f"❌ Error in main execution: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 SUCCESS: Data loading completed!")
        print("Your database is now ready for Power BI dashboard connection.")
    else:
        print("\n❌ FAILED: Data loading encountered errors.")