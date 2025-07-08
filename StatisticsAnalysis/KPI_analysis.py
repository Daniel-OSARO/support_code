import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
from tkinter import Tk, filedialog
from statsmodels.stats.outliers_influence import variance_inflation_factor

def analyze_kpi():
    # Open file selection dialog
    root = Tk()
    root.withdraw() # Hide Tkinter window
    file_path = filedialog.askopenfilename(
        initialdir=".", # Changed to current directory
        title="Select the CSV file for analysis",
        filetypes=[("CSV files", "*.csv")]
    )

    if not file_path:
        print("No file selected. Analysis terminated.")
        return

    print(f"Selected file: {file_path}")

    try:
        # Add skipinitialspace=True to handle spaces after delimiters
        df = pd.read_csv(file_path, skipinitialspace=True)
    except Exception as e:
        print(f"Error reading the file: {e}")
        return

    # --- Debugging: Print initial Date column info ---
    print("\n--- Initial 'Date' column info ---")
    print(f"Date column dtype: {df['Date'].dtype}")
    print("First 5 entries of 'Date' column (raw):")
    print(df['Date'].head())
    print("------------------------------------")

    # Strip whitespace from 'Date' column before conversion
    df['Date'] = df['Date'].astype(str).str.strip()

    # Convert 'Date' column to datetime format with error coercion
    # User confirmed manual fix of YYYY to YY, so %m/%d/%y should work.
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%y', errors='coerce') # Use coerce for robustness

    # Identify and print problematic date entries
    problematic_indices = df[df['Date'].isna()].index
    if not problematic_indices.empty:
        print("\nFound problematic date entries (converted to NaT):")
        for idx in problematic_indices:
            # Access original column to show problematic string
            print(f"Row {idx}: Original string was '{df.loc[idx, 'Date']}'")
        print("Please check these rows in your CSV file for unexpected characters or formats.")

    # Filter data after May 1, 2025
    start_date = pd.to_datetime('2025-05-01')
    df_filtered = df[df['Date'] >= start_date].copy() # Use .copy() to prevent SettingWithCopyWarning

    # Define a robust numeric conversion function
    def clean_and_convert_to_float(series):
        # Convert to string, remove commas, remove percent signs, then convert to numeric
        return pd.to_numeric(
            series.astype(str).str.replace(',', '', regex=False).str.replace('%', '', regex=False),
            errors='coerce'
        )

    # NEW: Convert 'Item Shipped' to numeric and filter
    if 'Item Shipped' in df_filtered.columns:
        print(f"\n[DEBUG] df_filtered shape before Item Shipped filter: {df_filtered.shape}")
        df_filtered['Item Shipped'] = clean_and_convert_to_float(df_filtered['Item Shipped'])
        df_filtered = df_filtered[df_filtered['Item Shipped'] >= 1500].copy() # Apply Item Shipped filter
        print(f"[DEBUG] df_filtered shape after Item Shipped filter: {df_filtered.shape}")
    else:
        print("\n[WARNING] 'Item Shipped' column not found in the data. Skipping filter for 'Item Shipped'.")

    # Calculate and add 'Total item stuck rate' by applying robust conversion to components
    item_stuck_rate_cleaned = clean_and_convert_to_float(df_filtered['Item stuck rate'])
    out_of_sow_item_stuck_rate_cleaned = clean_and_convert_to_float(df_filtered['Out of SOW scope item stuck rate'])
    df_filtered['Total item stuck rate'] = item_stuck_rate_cleaned + out_of_sow_item_stuck_rate_cleaned

    # Select required columns
    required_cols = [
        'Date',
        'Active UPH',
        'Pick Success',
        'Bagger error rate(Before August is QE)',
        'Total item stuck rate',
        'Item drop rate'
    ]

    # Create a DataFrame with only the selected columns
    df_analysis = df_filtered[required_cols].copy() # Added .copy() to prevent SettingWithCopyWarning

    # Convert other required columns to float (handle % and commas)
    cols_to_convert_to_float = [
        'Active UPH',
        'Pick Success',
        'Bagger error rate(Before August is QE)',
        'Item drop rate'
    ]
    for col in cols_to_convert_to_float:
        if col in df_analysis.columns:
            df_analysis[col] = clean_and_convert_to_float(df_analysis[col])

    # --- Debugging: Print dtypes after numeric conversion ---
    print("\n[DEBUG] dtypes after numeric conversion:")
    print(df_analysis.dtypes)
    print("------------------------------------")

    # Remove rows with any missing values (NaN), including those from date and numeric coercion
    df_analysis = df_analysis.dropna()

    if df_analysis.empty:
        print("No data available for analysis after filtering and dropping missing values.")
        return

    # Perform multiple regression analysis
    y = df_analysis['Active UPH']
    X = df_analysis[[
        'Pick Success',
        'Bagger error rate(Before August is QE)',
        'Total item stuck rate',
        'Item drop rate'
    ]]

    # Add a constant (intercept) to the regression model
    X = sm.add_constant(X)

    model = sm.OLS(y, X)
    results = model.fit()

    print("\n### Multiple Regression Analysis Results ###")
    print(results.summary())

    # --- VIF Analysis ---
    print("\n### Variance Inflation Factor (VIF) ###")
    vif_data = pd.DataFrame()
    vif_data["feature"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    print(vif_data.round(2))
    print("-------------------------------------")

    # Visualization
    plt.figure(figsize=(15, 10))

    # 1. Scatter plot of Actual vs. Fitted Values
    plt.subplot(2, 2, 1)
    sns.scatterplot(x=results.fittedvalues, y=y)
    plt.plot([y.min(), y.max()], [y.min(), y.max()], 'r--')
    plt.title('Actual vs. Fitted Values')
    plt.xlabel('Fitted Values (Predicted Active UPH)')
    plt.ylabel('Actual Active UPH')

    # 2. Residual plot
    plt.subplot(2, 2, 2)
    sns.scatterplot(x=results.fittedvalues, y=results.resid)
    plt.axhline(0, color='r', linestyle='--')
    plt.title('Residuals vs. Fitted Values')
    plt.xlabel('Fitted Values (Predicted Active UPH)')
    plt.ylabel('Residuals')

    # Display the first figure
    plt.tight_layout()
    plt.show()

    # --- New: Individual correlation plots with baseline --- #
    plt.figure(figsize=(18, 12)) # Larger figure for 4 plots

    # Plot 1: Active UPH vs. Bagger error rate
    plt.subplot(2, 2, 1)
    sns.scatterplot(x=df_analysis['Bagger error rate(Before August is QE)'], y=df_analysis['Active UPH'])
    plt.axhline(300, color='red', linestyle='--', label='Active UPH = 300 Baseline') # Baseline
    plt.title('Active UPH vs. Bagger Error Rate')
    plt.xlabel('Bagger Error Rate')
    plt.ylabel('Active UPH')
    plt.legend()

    # Plot 2: Active UPH vs. Pick Success
    plt.subplot(2, 2, 2)
    sns.scatterplot(x=df_analysis['Pick Success'], y=df_analysis['Active UPH'])
    plt.axhline(300, color='red', linestyle='--', label='Active UPH = 300 Baseline') # Baseline
    plt.title('Active UPH vs. Pick Success')
    plt.xlabel('Pick Success')
    plt.ylabel('Active UPH')
    plt.legend()

    # Plot 3: Active UPH vs. Total item stuck rate
    plt.subplot(2, 2, 3)
    sns.scatterplot(x=df_analysis['Total item stuck rate'], y=df_analysis['Active UPH'])
    plt.axhline(300, color='red', linestyle='--', label='Active UPH = 300 Baseline') # Baseline
    plt.title('Active UPH vs. Total Item Stuck Rate')
    plt.xlabel('Total Item Stuck Rate')
    plt.ylabel('Active UPH')
    plt.legend()

    # Plot 4: Active UPH vs. Item drop rate
    plt.subplot(2, 2, 4)
    sns.scatterplot(x=df_analysis['Item drop rate'], y=df_analysis['Active UPH'])
    plt.axhline(300, color='red', linestyle='--', label='Active UPH = 300 Baseline') # Baseline
    plt.title('Active UPH vs. Item Drop Rate')
    plt.xlabel('Item Drop Rate')
    plt.ylabel('Active UPH')
    plt.legend()

    # Display the second figure
    plt.tight_layout()
    plt.show()

# Run the analysis function
if __name__ == '__main__':
    analyze_kpi()
