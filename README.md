<p align="left">
  <img src="https://nci.org.au/themes/custom/nci/logo.svg" alt="Company Logo" width="100">
</p>

<h1 align="center">nf-gadi Plugin</h1>

`nf-gadi` plugin provides a usage report for nextflow processes using NCI Gadi PBS Pro executor. To run this plugin, Nextflow version must >= 24.04.1. 

## Installation
### Install from plugin registry
Start from v1.1.0, this plugin can be download from [Nextflow plugin registry](https://registry.nextflow.io/plugins/nf-gadi)

```
nextflow plugin install nf-gadi@1.2.0   
```

We recommend to download the plugin before running the workflow. 

### Install from repository
To use this plugin in an offline environment, you will need to download and install this plugin on a system with an internet connection.  

Clone the repository
```
git clone https://github.com/eileen-xue/nf-gadi.git
```
Build the plugin
```
cd nf-gadi
module load nextflow
make install
```
## Settings
`nf-gadi` can provide json or csv output. Users can define the output format and output file name in the `nextflow.config` file. Both settings are optional. By default, the plugin generates a `UsageReport.csv` file. Do not change the output file name if requires to get cached usage report data with nextflow `-resume`.
```
gadi {
    format = 'csv'
    output = 'report.csv'
}
```
## Run the plugin
There are two methods to run the Nextflow pipeline with the plugin
### nextflow.config
Add the plugin to the configuration file 
```
plugins {
    id 'nf-gadi@1.2.0'
}
```
Then run the Nextflow pipeline with `nextflow run main.nf`

### Nextflow command 
Another method to run the plugin is by adding it to the Nextflow command 
``` 
nextflow run main.nf -plugins nf-gadi@1.2.0
```
