import pandas as pd 

#Read json data using pandas 
df = pd.read_json("loan_approval_dataset.json")

#Export the dataframe to a csv in the data folder
df.to_csv("data/loan.csv", index=False)