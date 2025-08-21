# ccil_scraping_pipeline

This is the repository for the CCIL Scraping Pipeline.  

The CCIL API is scraped every hour and data is appended to the ccil_securities table in the Azure SQL DB. This is automated using crontab and an Ubuntu 18.04 VM on Azure.  
The dashboard allows users to select a Security description to view its daily trades and tta. This is deployed on Azure's App service.

## Logging
Cronjob logs are stored under cron.log in the VM.

## Dashboard Link
https://ccil-dashboard.azurewebsites.net/