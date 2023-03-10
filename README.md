# STORM SURGE RISK ANALYSIS

To estimate the number of affected buildings(people) in a storm surge induced flood event, we combine storm surge level,
flood extent, and exposure data.

Here are the steps which are implemented in this data pipeline :

  1. Obtain storm surge level forecast data: The storm surge forecast data data come from Global Storm Surge Information System
    [GLOSSIS]( https://www.deltares.nl/en/projects/global-storm-surge-information-system-glossis/) The storm surge water level data will provide information on the depth of floodwater at a particular location.
  2. Determine flood extent: The flood extent data come from the national operational assessment of hazards
    [NOAH project](https://noah.up.edu.ph/).      The flood extent data will provide information on the geographical extent of the flood induced by the storm surge event.
  3. Identify buildings at risk: Exposure data for this assessment is obtained from the [google building footprint data](https://sites.research.google/open-buildings/).
    This data was used to identify buildings that are located within the forecasted flood extent area and are therefore at risk of being affected by storm surge induced flooding.
  4. Overlay the data: Once we obtained the forecasted storm surge level data, flood extent data, and exposure data,
    we overlay these layers to determine the number of buildings that are at risk of being affected by the forecasted flooding.
  5. The storm surge advisory maps developed by NOAH has different risk categories, the final output of the analysis provides an
    estimated number of buildings for each risk categories. The buildings at risk are reported at municipality level

It is important to note that the accuracy of the estimated number of affected buildings will depend on the accuracy of the data sources used.

## IBF-pipeline Floods
This pipeline calculate storm surge risk forecast in the Philipines based on the steps discribed above
The pipeline consists of a series of Python scripts, which are supposed to run daily, to:
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
- Change `secrets.env.template` to `secrets.env` and fill in the necessary secrets. Particularly fill in for
  - GLOSSIS_USER: retrieve from someone who knows
  - GLOSSIS_PW: retrieve from someone who knows

- Go to the root folder of the repository
- Build and run Docker image: `docker-compose up --build`
- (Optional) When you are finished, to remove any docker container(s) run: `docker-compose down`

The resulsts from pipeline will be uploaded to 510 datalack. The latest model run results can be downloade from [here](https://510ibfsystem.blob.core.windows.net/ibftyphoonforecast/ibf_model_results/ss_model_outputs.zip)

### 2. Github Actions
Getting its secrets from Github Secrets.

- Fork this repository to your own Github-account.
- Add Github secrets in Settings > Secrets of the forked repository: `https://github.com/<your-github-account>/<repo name>/settings/secrets/actions`
  - Add the same secrets as mentioned in the local scenario above.
- The Github action is already scheduled to run daily at a specific time. So wait until that time has passed to test that the pipeline has run correctly.
  - This time can be seen and changed if needed in the 'on: schedule:' part of [floodmodel.yml](.github/workflows/floodmodel.yml), where e.g. `cron:  '0 8 * * *'` means 8:00 AM UTC every day.

### Acknowledgments

- [Start Network](https://startnetwork.org/)
