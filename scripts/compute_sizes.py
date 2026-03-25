import pandas as pd
import spectrabrainz
from pandarallel import pandarallel

pandarallel.initialize(nb_workers=20, progress_bar=True)

df = pd.read_csv("summary_metadata.tsv", sep="\t", usecols=["bildid", "size", "number_of_files"])
df["storcycle_size"] = df["bildid"].parallel_apply(spectrabrainz.get_size)

df = df[["bildid", "number_of_files", "storcycle_size", "size"]].sort_values("number_of_files", ascending=False)

df["storcycle_size"] = df["storcycle_size"] / 2
df["compression_ratio"] = df["size"] / df["storcycle_size"]
df["savings_pct"] = (1 - df["storcycle_size"] / df["size"]) * 100

df = df.dropna(subset=["savings_pct"])

df.to_csv("compression_report.tsv", sep="\t", index=False)
