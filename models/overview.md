{% docs __overview__ %}
# Elementary data
You can find [here](./elementary_report.html) the Elementary page.

# Introduction
This project provides predefined data analytics content for selected SAP financial insights. Includes:

### Sales Performance


### Order Fulfillment


### Billing and Pricing


### Order Information


Using dbt, it transforms all the necessary SAP tables into the ordertocash table. Dbt is connected to Snowflake where all source and target tables are stored. Ingesting SAP tables into Snowflake isn't part of the project.

# How to start
First you need clone repository
```
git clone
```
Then set environment and Snowflake connection.

## Environment Settings


```

```

## Snowflake Connection
In file `models/schema.yml` set Snowflake database in which all required SAP tables are prepared.

In file `profiles.yml` set your Snowflake and destination catalog and schema.


Run project
```
dbt run
```
Now you should have ordertocash table in your Snowflake.

{% enddocs %}
