> Warning: This is the README for the publicly accessible version of the POMI package. If you are an analyst, please don't use the below instructions to run the publication process.

<p>&nbsp;</p>

# POMI RAP 

## Contact Information

Repository owner: Primary Care Domain Analytical Team

Email: primarycare.domain@nhs.net

To contact us raise an issue on Github or via email and we will respond promptly.

<p>&nbsp;</p>

## Publication Summary

Patient Online is an NHS England programme designed to support GP Practices to offer and promote online services to patients, including access to coded information in records, appointment booking and ordering of repeat prescriptions. Data are provided by GP system suppliers to NHS England monthly and published on the last Thursday each month, pending no issues, otherwise as soon as possible thereafter.

The publications can be found here:

https://digital.nhs.uk/data-and-information/publications/statistical/mi-patient-online-pomi/current

<p>&nbsp;</p>

## Set up

1. Make a [clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) of the repository on your local machine.
2. Navigate to the cloned version of the repository in a command line terminal by setting your current directory to the location of the cloned repository.
3. Create and activate the correct environment by entering the below two sperate commands into your terminal:
```
conda env create --name pomi --file requirements.txt
```
```
conda activate pomi
```
Note that you only need to create your environment once. If you wish to return to the project again, you can omit the 'conda env create' command.

<p>&nbsp;</p>

## Creating a file structure 

To run this process locally you will need to create the below file structure on your machine and insert the provided files in the 'Code' folder as instructed in the 'Instructions for producing publication' steps.

```
root
│
├───CODE
│
├───INPUTS
│   │
│   ├─── Fact Table Exclude List INF.csv
│   │
│   ├─── Fact Table Exclude List.csv
│   │
│   ├─── Trend Monitor Template.xlsx
│   │
│   ├─── GPWT_PARTICIPATION_DDMMYY.csv
│   │
│   ├─── QS Part Status-GPWC-YYYY-YYYY.csv
│       
├───OUTPUTS
```

<p>&nbsp;</p>

## Instructions for publication production

After the above set up steps have been completed you can follow the below instructions to create the publication. Please note that you will not be able to run the code as this requires access to a private server.
The data on the private server contains reference data that is used for mapping purposes. The reference tables used contain data from the [PCN API Call](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data), [GP Practice API call](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data) and [ONS code history database](https://www.ons.gov.uk/methodology/geography/geographicalproducts/namescodesandlookups/codehistorydatabasechd)

1. In the config file edit the root directory value so that it matches the root of the directory that you set up earlier. Make use of escape characters e.g., "\\\\example\\root\\directory". Make sure your Code, Inputs and Outputs files are there.
2. Ensure that all of the input files are in the INPUTS folder as per the file structure above.
3. Run the main file by typing the below command into your terminal (make sure your terminal has the current directory set to that of the cloned repo):
```
python -m main
```

After the process has run the output will be in the {root_directory}\Outputs. You can set root_directory in config.json to "" to automatically detect the current directory.

<p>&nbsp;</p>

> WARNING: Please note that python uses the '\\' character as an escape character. To ensure your inserted paths work insert an additional '\\' each time it appears in your defined path. E.g.,  'C:\Python25\Test scripts' becomes 'C:\\\Python25\\\Test scripts'

<p>&nbsp;</p>

## Licence
POMI codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the Open Government 3.0 licence.
