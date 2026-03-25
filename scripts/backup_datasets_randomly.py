import spectrabrainz
import pandas as pd

number_of_random_samples = 10

df = pd.read_csv('20260211.tsv', sep='\t')
df_sample = df.sample(n=number_of_random_samples, random_state=15213)

counter = 0
limit = 10

for index, datum in df.iterrows():
  if counter >= limit:
     break  # stop after 10 iterations

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
