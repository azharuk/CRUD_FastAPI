import pandas as pd

url = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"
courses_df = pd.read_csv(url)

# Check the first few rows
print("First 5 rows of the DataFrame:")
print(courses_df.head())

# Check the shape of the DataFrame
print("\nShape of the DataFrame:", courses_df.shape)

# Inspect data types
print("\nData types of each column:")
print(courses_df.dtypes)

# Summary statistics
print("\nSummary statistics:")
print(courses_df.describe(include='all'))

# Check for missing values
print("\nMissing values in each column:")
print(courses_df.isnull().sum())

# Display column names
print("\nColumn names:")
print(courses_df.columns)