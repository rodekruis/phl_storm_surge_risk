# IBF-pipeline Floods

This pipeline calculate storm surge risk

The pipeline consists of a series of Python scripts, which - if activated - are supposed to run daily, to:
- extract relevant forecast input data
- transform them to create flood extents
- calculate affected population


## Methods for running this pipeline

This pipeline is preconfigured to run in 3 different ways:

### 1. Locally
Getting its secrets from a local secrets.py file. 
Obviously, this method can be used also non-locally, e.g. by running it as cronjob on a Virtual Machine.

- Install Docker
- Clone this directory through 
- Change `pipeline/lib/flood_model/secrets.py.template` to `pipeline/lib/flood_model/secrets.py` and fill in the necessary secrets. Particularly fill in for 
  - GLOSSIS_USER: retrieve from someone who knows
  - GLOSSIS_PW: retrieve from someone who knows

- Go to the root folder of the repository
- Build and run Docker image: `docker-compose up --build`
- (Optional) When you are finished, to remove any docker container(s) run: `docker-compose down`



### 2. Github Actions
Getting its secrets from Github Secrets.

- Fork this repository to your own Github-account.
- Add Github secrets in Settings > Secrets of the forked repository: `https://github.com/<your-github-account>/<repo name>/settings/secrets/actions`
  - Add the same secrets as mentioned in the local scenario above.
- The Github action is already scheduled to run daily at a specific time. So wait until that time has passed to test that the pipeline has run correctly.
  - This time can be seen and changed if needed in the 'on: schedule:' part of [floodmodel.yml](.github/workflows/floodmodel.yml), where e.g. `cron:  '0 8 * * *'` means 8:00 AM UTC every day.



### 3. Azure logic app
Getting its secrets from Azure Key Vault.

- The Azure logic app needs to be set up separately, based on this repository.
- The logic to get the secrets from the Azure Key Vault is already included in the code. 
