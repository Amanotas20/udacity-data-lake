# Sparkify ETL
## 1. Context:
The main purpose of this database is to give Sparkify a well crafted data model in order to allow faster analytics.  
Sparkify, as a music streaming startup, they need a way to analize user trends and make statistical  
calculations.  
This can be achieved using Amazon's Redshift and a data handling pipeline with Python.

## 2. Schema and Pipeline
The proposed schema was based on Kimbals Star Schema, where we have a fact table and  
several dimensional tables in order to make joins and queries faster.  
The main dimensions for this case are Users, Artists, Songs and Time, most of the analysis surrounds those dimensions.  

The pipeline was intended to be the most simple and straightforward, extracting data and stage it in Redshift  
is a good way to use the service, we have the raw data and the modeled data in the same place which can  
help to faster modeling.