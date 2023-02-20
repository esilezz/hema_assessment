# HEMA technical assessment Data Engineer

This repo contains the proposed solution for the technical assessment for the position of data engineer with HEMA.

## Proposed solution

The solution has been implemented using `spark` and `docker`, as explained in the requirements. As `spark` has been chosen as framework, the solution has been implemented in a full `pyspark` fashion. The solution follows the functional and technical requirements:

- `raw` dataframe is the file as it is read, just augmented with the metadata column. It is saved in a csv format and it is partitioned by `Order Date`.
- `curated` dataframe is processed from the `raw` one. The dataframe is saved as parquet, and it is partioned on `orderDate`. The transformation consists in;
  - changing the name of columns into camelCase format
  - fixed the data types of the columns
  - augmented with metadata
- for the `consumption` layer, the dataframes `sales` and `customers` are in line with the requirements. The `sales` dataframe is written in `append` mode, `customers` in `overwrite` mode.

## How to run

To run the script, you need to have Docker installed on your machine.

Open a new command line shell in the directory `hema_assessment`, and run the following commands:

```
docker build . -t hema-assessment
docker run -v "<path/for/output/files>:/usr/app/src/output" hema-assessment
```

Please make sure to replace `<path/for/output/files>` with an actual path where you would like to save the output files (e.g. `/Users/<username>/Desktop/hema_assessment/output` on Mac or `C:\Users\<username>\Desktop\hema_assessment\output`).

Once the execution within the container is completed, you will be able to check the ouput in the location you chose for mounting the docker volume.
