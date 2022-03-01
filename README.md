# Detock Analysis

This repository contains Jupiter notebooks for analyzing experimental data for the paper on [Detock](https://github.com/ctring/Detock).

Raw data can be downloaded from [here](https://drive.google.com/drive/folders/1rDCPg2U5M4ewdge2-7QBJFT5UDPn2UNx?usp=sharing).

The raw data is organized by experiment names. After downloading data for an experiment (e.g. `tpcc.tar.gz`), put it in the root directory and untar the file:

```
tar -xzvf tpcc.tar.gz
```

After running this commands, the data will be placed under the `main` directory (e.g. `main/tpcc`). The final raw data files are still compressed under this tree. To decompress all of these files, run the following command:

```
python3 untar.py main/tpcc
```

Without any raw data, the notebooks can still use the derived data cache files to generate figures. A cell that can read from either the cache files or the raw data has the variable `IGNORE_CACHE`, which can be set to `True` to make the cell read raw data.






