# Intive Challenge

This project consists of analyzing sales data from a chain of stores, it is based in Spark for distributed processing.

## The problem

To ingest a set of data for analizying sales data from stores and calculate aggregate metrics, as no dataset was provided and today is saturday, I've created the dataset myself using a [fake_data_generator](./helpers/gen_samples.py)

1. The generator stores data under [data](./data/) dir

2. The raw data is loaded as a Spark dataframe to abstract the power and speed of distributed processing.

3. The data is then consumed using a FastAPI, as this was my first time developing an API using Python, I hope having implemented all the necessary steps.

4. The root endpoint presents a brief documentation of the project, describing the possible endpoints and it's parameters.

### How to get started

If data directory is empty, you will need to generate some fake data to be analyzed, by typing:

```bash
python ./helpers/gen_samples.py
```

As soon as you have data, let's start the API. The main file is responsible to:

1. Start the spark session
2. Load the .csv raw data you just generated into a Spark DataFrame and performe some data cleaning.
3. Serve the API to consume the clean data performing some aggregations.

If you want to perform analysis by yourself, there is a [Jupyter Notebook](./data-analyzer.ipynb) for that purpose.

If you want to proceed with the API serving, just type:

```bash
python main.py
```
