import spectrabrainz as spectra
import pandas as pd
from tqdm import tqdm

df = pd.read_csv('summary_metadata.tsv', sep='\t')
df = df[df['number_of_files']!=1]
df = df[df['number_of_files']<=100]

counter = 0
limit = 500

for index, datum in tqdm(df.iterrows()):
  if counter >= limit:
     break  # stop after 10 iterations

  name = datum['bildid']
  description = datum['affiliation']
  directory = datum['bildirectory'].replace('/bil/data/','/')

  if not spectra.exists(name):
     print(f'Dataset with name {name} does not exist in StorCycle.')
     print(f'Creating project with name {name} and scanning')
     data = spectra.create(name=name, description=description, directory=directory, token=spectra.login())
     counter += 1
