library(tidyverse)

dataset<- person

dataset <- dataset %>% mutate(
  PHD= if_else(person_id %in% I27_all$person_id, TRUE, FALSE),
  AC1=if_else(person_id %in% ICD_PH$person_id, TRUE, FALSE),
  Clean_PAH1 = if_else(person_id %in% CleanPAH_1$person_id ,TRUE,FALSE),
  Clean_PAH2 = if_else(person_id %in% CleanPAH_2$person_id ,TRUE,FALSE)
                         )
# load relatedness and remove related samples
system("gsutil -u $GOOGLE_PROJECT cp gs://fc-aou-datasets-controlled/v7/wgs/short_read/snpindel/aux/relatedness/relatedness_flagged_samples.tsv .", intern=T)

relatedness_flagged_samples <- read_csv("relatedness_flagged_samples.tsv")

length(intersect(dataset$person_id,relatedness_flagged_samples$sample_id))

#  

dataset <- dataset %>% filter(!person_id %in% relatedness_flagged_samples$sample_id)


# save dataset
## setup snippet
  
lapply(c('tidyverse', 'bigrquery'),
       function(pkg_name) { if(! pkg_name %in% installed.packages()) { install.packages(pkg_name)} } )

## BigQuery setup.
BILLING_PROJECT_ID <- Sys.getenv('GOOGLE_PROJECT')
# Get the BigQuery curated dataset for the current workspace context.
CDR <- Sys.getenv('WORKSPACE_CDR')

###

# This code saves your dataframe into a csv file in a "data" folder in Google Bucket

# Replace df with THE NAME OF YOUR DATAFRAME
my_dataframe = dataset   

# Replace 'test.csv' with THE NAME of the file you're going to store in the bucket (don't delete the quotation marks)
destination_filename = 'definitions.csv'

########################################################################
##
################# DON'T CHANGE FROM HERE ###############################
##
########################################################################

# save dataframe in a csv file in the same workspace as the notebook
write_excel_csv(my_dataframe, destination_filename)

# Get the bucket name
my_bucket <- Sys.getenv('WORKSPACE_BUCKET')

# Copy the file from current workspace to the bucket
system(paste0("gsutil cp ./", destination_filename, " ", my_bucket, "/data/"), intern=T)

# Check if file is in the bucket
system(paste0("gsutil ls ", my_bucket, "/data/*.csv"), intern=T)


