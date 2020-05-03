import pandas as pd

def trunc(created_on):
    return created_on.split()[0]

f=pd.read_csv("op-go.csv")
keep_col = ['lat','lon', 'created_on']
new_f = f[keep_col]
new_f["created_on"] = new_f["created_on"].apply(trunc)
new_f.to_csv("new-decoded.csv", index=False)
