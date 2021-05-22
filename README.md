# Apache Ignite Migration Tool

Apache Ignite Migration Tool is an open-source library for applying Apache Ignite database schema changes.

[![Coverage Status](http://codecov.io/github/Alliedium/ignite-migration-tool/coverage.svg?branch=main)](http://codecov.io/github/Alliedium/ignite-migration-tool?branch=main)
![CI](https://github.com/Alliedium/ignite-migration-tool/actions/workflows/main.yml/badge.svg)

## Features
 - The data migration is performed in 3 stages:
   1. exporting data and meta data from a live Apache Ignite cluster into an isolated filesystem directory in form of Avro files.
   2. applying database schema transformations to the exported data and writing the transformed data into a separate filesystem directory.
   3. uploading the transformed Avro files to the new cluster.
 - data and metadata transformations are defined in a way that is neither Apache Ignite nor Avro-specific (which allows for potential use of [Apache Beam](https://beam.apache.org/) for applying database transformations).
 -  data and metadata transformations are applied to avro files and do not require a live Apache Ignite cluster.
 -  the tool can be used for creating Apache Ignite data backups that are both version and topology-independent. Cache metadata is backed up (as xml configuration) along with cache data 


## Assumptions
 - Apache Ignite cluster from which the data is migrated from and the cluster to which the data is migrated to should be different clusters
 - All nodes of Apache Ignite cluster to which the data is migrated to should have access to new defintions of all data classes (i.e. corresponding to the transformed schema). This can be achived by placing jar file with new data classes definitions to each node's class path. It is the tool's user responsibility to make sure that this assumption holds, the tool doesn't automate this process in any way.
 - Each cache is configured via `QueryEntity`, all cache fields not present in `QueryEntity` are invisible to the tool and won't be backed up.
 - Caches/tables that are not persisted in Ignite Persistence (in-memory only caches) are backed up by the tool along with the persisted caches. It is up to the data transformation patch (see [here](https://github.com/Alliedium/ignite-migration-tool/blob/main/products/demo/src/main/java/org/alliedium/ignite/migration/patches/AlterCachesDemoPatch.java) for a patch example) to make sure that in-memory caches are ignored (if needed) upon deserialization from Avro.


## Getting started
The tool cannot be considered as the boxed product with a user-friendly CLI just yet. However, it is designed as a set re-usable building blocks with well-defined interfaces between them. The best way to get yourself familiar with the tool is to run [`demo/run_demo.sh`](https://github.com/Alliedium/ignite-migration-tool/blob/main/products/demo/run_demo.sh) script and study the source code from top to the bottom (starting with the script). We assume that users of the tool will be able to clone the repository and tailor-fit the tool to their needs.
