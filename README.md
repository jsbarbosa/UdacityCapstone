# Udacity Capstone Project

## Data
### [Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)
This data comes from the US National Tourism and Trade Office

### [Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/Demographics)
This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 

This data comes from the US Census Bureau's 2015 American Community Survey.

## Input Dictionary
### Inmmigration data
- `I94YR`: 4 digit year
- `I94MON`: Numeric month
- `I94CIT`: This format shows all the valid and invalid codes for processing 
  (code values in `data/I94CIT_I94RES.csv`)
- `I94RES`: This format shows all the valid and invalid codes for processing
  (code values in `data/I94CIT_I94RES.csv`)
- `I94PORT`: This format shows all the valid and invalid codes for processing
  (code values in `data/I94PORT.csv`)
- `ARRDATE`: is the Arrival Date in the USA. It is a SAS date numeric field that a
   permament format has not been applied.  Please apply whichever date format
   works for you
- `I94MODE`: There are missing values as well as not reported (9)
  (code values in `data/I94MODE.csv`)
- `I94ADDR`: There is lots of invalid codes in this variable and the list below
   shows what we have found to be valid, everything else goes into 'other'
  (code values in `data/I94ADDR.csv`)
- `DEPDATE`: is the Departure Date from the USA. It is a SAS date numeric field that 
  a permament format has not been applied.  Please apply whichever date format
  works for you
- `I94BIR`: Age of Respondent in Years
- `I94VISA`: Visa codes collapsed into three categories
  (code values in `data/I94VISA.csv`)
- `COUNT`: Used for summary statistics
- `DTADFILE`: Character Date Field - Date added to I-94 Files - CIC does not use
- `VISAPOST`: Department of State where where Visa was issued - CIC does not use
- `OCCUP`: Occupation that will be performed in U.S. - CIC does not use
- `ENTDEPA`: Arrival Flag - admitted or paroled into the U.S. - CIC does not use
- `ENTDEPD`: Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
- `ENTDEPU`: Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
- `MATFLAG`: Match flag - Match of arrival and departure records
- `BIRYEAR`: 4 digit year of birth
- `DTADDTO`: Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
- `GENDER`: Non-immigrant sex
- `INSNUM`: INS number
- `AIRLINE`: Airline used to arrive in U.S.
- `ADMNUM`: Admission Number
- `FLTNO`: Flight number of Airline used to arrive in U.S.
- `VISATYPE`: Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

## Demographics (`data/us-cities-demographics.csv`)
- `City`: Corresponding city
- `State`: Corresponding state
- `Median Age`: Medium age of the population in the city/race
- `Male Population`: Number of males in the city/race
- `Female Population`: Number of females in the city/race
- `Total Population`: Total population in the city/race
- `Number of Veterans`: Number of veterans in the city/race
- `Foreign-born`: Number of foreign born in the city/race
- `Average Household Size`: Average household size
- `State Code`: Code of the state
- `Race`: Race
- `Count`: 

# Configuration
1. Go to [http://localhost:3000/admin/connection/](http://localhost:3000/admin/connection/)
and click Create.
2. Add and AWS connetion
- **Conn Id:** `aws`
- **Conn Type:** `Amazon Web Services`
- **Login:** `<ACCESS_KEY>`
- **Password:** `<SECRET_ACCESS>`
3 Add an Spark connetion
- **Conn Id:** `spark`
- **Conn Type:** `Spark`
- **Host:** `spark://<EMR_USERNAME>`
- **Port:** `7077`

4. Add a variable called `raw_data` in airflow, pointing to the location of the 
project data folder. It can be obtained by running: `echo "$PWD/data"`