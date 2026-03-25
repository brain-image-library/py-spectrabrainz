import spectrabrainz as spectra
import pandas as pd

df = pd.read_csv('summary_metadata.tsv', sep='\t')

counter = 0
limit = 250

for index, datum in df.iterrows():
  if counter >= limit:
     break

  name = datum['bildid']
  description = datum['affiliation']
  directory = datum['bildirectory'].replace('/bil/data/','/')

  if spectra.exists(name):
     print(f'Dataset with name {name} already exists in StorCycle')
  else:
     print(f'Dataset with name {name} does not exist in StorCycle.')
     print(f'Creating project with name {name} and scanning')
     data = spectra.create(name=name, description=description, directory=directory, token=spectra.login())
     counter += 1
