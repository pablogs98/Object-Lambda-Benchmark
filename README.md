# S3 Object Lambda measurement

This repository contains the source of the microbenchmarks and use cases featured in the research paper _On Data Processing Through the Lenses of S3 Object Lambda_, from IEEE INFOCOM 2023.


## Getting started

The benchmarks and use cases have been written and executed using **Python 3.8** running on **Ubuntu 20.04**.

### Setup

1. Clone this repository on your local machine:

```bash
git clone https://github.com/pablogs98/Object-Lambda-Benchmark
```

2. From the repository's root directory, install its Python dependencies:

```bash
pip3 install -r requirements.txt
```

3. Make sure that your AWS account and AWS CLI are correctly set up. More information available [here](https://aws.amazon.com/cli/?nc1=h_ls).

4. Install any additional dependencies. For instance, PycURL has additional requirements, namely, [libcurl](https://curl.se/libcurl/). 

5. Make sure ```PYTHONPATH``` points to the repository's root directory.

### Deploying functions

Functions are automatically deployed when an example is executed. However, the deployment packages must be generated beforehand and located in the root directory of the microbenchmark/use case (or within a configurable, specified path). In the [utils](object_lambda_benchmark/utils) module, we provide scripts which take care of the generation of the deployment packages for Node.js and Python. 

More information on Java function deployments [here](https://docs.aws.amazon.com/lambda/latest/dg/java-package.html).

### Datasets

The datasets used for experimentation are publicly available and can be downloaded in the following locations:

| Use case      | Dataset |
| ----------- | ----------- |
| Grep | [GHTorrent](https://ieeexplore.ieee.org/abstract/document/6624034)       |
| Parallel tree reduction (streaming pipelines)  | [HDFS logs](https://dl.acm.org/doi/abs/10.1145/1629575.1629587) |

## References

Pablo Gimeno Sarroca, Marc Sànchez-Artigas. [On Data Processing Through the Lenses of S3 Object Lambda](), in IEEE INFOCOM 2023.

## Acknowledgements
<img src="https://user-images.githubusercontent.com/45240979/228180946-606cb75e-46c9-429c-a62b-ea9098c375a0.svg"  height="65">

This project has received funding from the European Union's Horizon Europe (HE) Research and Innovation Programme (RIA) under Grant Agreement No. **101092646** and No. **101092644**.
