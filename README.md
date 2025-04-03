# Code to generate prevelance changes based on the National Child Measurement Program. #
📉📉 hopefully we're getting those prevelances 📉📉

* **"0.functions.R"** - generic functions which select variables to load in and transform data with dynamic naming. 
* **"1.loading_and_cleaning.R"** - specify in this file the sample size, year group and survey year of interest. It currently reads in 10k observations from 2023/24, 2022/23 and 2021/22. 
* **"2.run_model.R"** - Ctrl+A -> Ctrl+Enter ,  this runs the whole model and saves the prevelance tables in "outputs" it will be named the year group and sample size. 

The numbers before the .R files signal the flow of the code, no need to run all seperately as they are sourced in appropriately. 
