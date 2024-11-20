# Project Data Engineering

## Description
This project is an application that was wrtitten as an assigment for Data Engineering course in IU.

## How to run it
To run project just simply 
```
docker-compose up -d
```
As the result:
* MongoDB container
* Mongo Express web client [localhost:8081](localhost:8081) 
* Loader application 
    * This container will be stoped as soon as data will be uploaded

## Limitations
* Script in current form can handle only CSV files as the source.
* Script is not veryfing if data is already uploaded into MongoDB.

## Adding new data

In folder `data` add subfolder and put csv files in it

## Presitent data after removing containers
In `docker-compose.yml` file uncomment line 13 and 14. Data from Mongo will presit on disk in `mongo_data` catalog that you can find in this repository

## Data Sources

### type 1 
https://www.kaggle.com/datasets/budincsevity/szeged-weather?resource=download

### type 2
https://www.kaggle.com/datasets/mattpo/temperature-iot-on-gcp

Data files ware renamed for simplicity and splited into smaller files due to GitHub restrictions [link](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github)