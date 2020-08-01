import pandas as pd

def trunc(created_on):
    return created_on.split()[0]

#f=pd.read_csv("op-go.csv")
#keep_col = ['lat','lon', 'created_on']
#new_f = f[keep_col]
#new_f["created_on"] = new_f["created_on"].apply(trunc)
#new_f.to_csv("new-decoded.csv", index=False)

#f=pd.read_csv("non-gridded.csv")
#keep_col = ['lat','lon', 'created_on']
#new_f = f[keep_col]
#new_f["created_on"] = new_f["created_on"].apply(trunc)
#new_f.to_csv("new-decoded.csv", index=False)

#BBox = (77.4251, 77.7993, 12.8570, 13.0862)

df = pd.read_csv("non-gridded.csv")
heat_df = df.copy()
heat_df = heat_df[['lat', 'lon']]
#heat_df = heat_df[(heat_df.lon >= BBox[0]) & (heat_df.lon <= BBox[1]) & (heat_df.lat >= BBox[2]) & (heat_df.lat <= BBox[3])]
#heat_df["created_on"] = heat_df["created_on"].apply(trunc)
heat_df.to_csv("non-gridded-stripped.csv", index=False)
