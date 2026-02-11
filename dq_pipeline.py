import pandas as pd
import great_expectations as gx
import sys
import os
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToBeBetween
)

def run_dq():
    # 1. Load Data
    csv_path = "data/amazon_orders.csv"
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]

    # 2. GX Setup
    context = gx.get_context()
    ds = context.data_sources.add_pandas(name="my_ds")
    asset = ds.add_dataframe_asset(name="my_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("batch_def")
    
    suite = context.suites.add(gx.ExpectationSuite(name="prod_suite"))

    # 3. Expectations
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="order_id"))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="qty", min_value=0))

    # 4. Run Validation
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="v_def", data=batch_def, suite=suite)
    )
    
    result = val_def.run(batch_parameters={"dataframe": df})

    # 5. Summary & Exit
    print(f"📊 Success Rate: {result.statistics['success_percent']}%")

    if not result.success:
        print("🚨 DATA QUALITY FAILED!")
        # If any row is bad, we kill the process to turn the GitHub Action RED
        sys.exit(1) 
    
    print("✅ DATA QUALITY PASSED!")
    sys.exit(0)

if __name__ == "__main__":
    run_dq()
