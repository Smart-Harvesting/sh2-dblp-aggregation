# Data Aggregation for dblp Monitoring

## VM Arguments

For the XML parser, the entity expansion limit given by the VM is not enough, so you need to override it in the VM arguments.
A reasonable choice would be so set no limit at all, thus `-DentityExpansionLimit=0`.

It is also advised to provide the program with enough memory, e.g. `-Xmx9g` or more.

## Dependencies

This project has some dependencies on other projects that are dblp-internal and not publicly available.  
That is, to run the application, you need to have the following projects on the build path:

- apache-log-analysis
- dblp-citations
- dblp-commons
- mmdb
- streams

Ask dblp developers for access.

## Setting up the Database

To create the database with the parameters and input files specified, [AggregationApplication](src/main/java/de/th_koeln/iws/sh2/aggregation/AggregationApplication.java) needs to be executed.

A PostgreSQL database is used to store the aggregated data.

The database parameters have to be specified in database.properties file.
The [database.properties.default](src/main/resources/config/database.properties.default) file serves as a template.

```
host=hostname
port=port
dbname=dbname
user=user
password=password
```

### Input Data

The following files are needed for the program to build the database:

* bht.xml
* dblp.xml
* hdblp.xml
* streams.xml
* dblp.dtd
* folder containing auxiliary data (mainly citation info) mapped from OAG/MAG to dblp

The paths to these files have to be specified in setup.properties file.
The [setup.properties.default](src/main/resources/config/setup.properties.default) file serves as a template.

```
xml.bht=path/to/bht.xml
xml.dblp=path/to/dblp.xml
xml.hdblp=path/to/hdblp.xml
xml.streams=path/to/streams.xml

dtd.dblp=path/to/dblp.dtd

path.oaga=path/to/oag-mag-aux-root-dir

output.basedir=path/to/output/basedir
```

Take care that all files containing dblp data are from the same date.
