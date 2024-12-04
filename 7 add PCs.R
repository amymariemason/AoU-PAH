lapply(c('skimr', "sessioninfo", "bigrquery", "lubridate","skimr","vctrs","tidyverse"),
       function(pkg_name) { if(! pkg_name %in% installed.packages()) { install.packages(pkg_name)} } )

library(bigrquery)
library(lubridate)
library(skimr)
library(tidyverse)
library(dplyr)
library(tidyr)
library(stringr)

# define names
# Papermill parameters. See https://papermill.readthedocs.io/en/latest/usage-parameterize.html
DATESTAMP <- strftime(now(), '%Y%m%d')
my_bucket <- Sys.getenv('WORKSPACE_BUCKET')

#---[ Inputs ]---
AOU_PHENO <- paste0(my_bucket, "/data/", "definitions.csv")

#---[ Outputs ]---
# Create a timestamp for a folder of results generated today.

DESTINATION <- str_glue('{BUCKET}/data/pheno/{DATESTAMP}/')

#is this the pheno name for the ouput of this notbook, using the write_csv function but not sure if Regenie will like
PHENOTYPE_FILENAME <- 'definitions_with_PCS.txt'

# find ancestry definitions
system("gsutil -u $GOOGLE_PROJECT du -h -c gs://fc-aou-datasets-controlled/v7/wgs/short_read/snpindel/aux/ancestry/", intern=T) 

# copy ancestry definitions to local file 
system("gsutil -u $GOOGLE_PROJECT cp gs://fc-aou-datasets-controlled/v7/wgs/short_read/snpindel/aux/ancestry/ancestry_preds.tsv .", intern=T)

Anc <- read_tsv('ancestry_preds.tsv')
Anc_pca <- data.frame(Anc[,c(1,4)])
Anc_str <- data.frame(str_split_fixed(Anc_pca$pca_features, ', ', 16))
colnames(Anc_str) <- paste0("PC", seq_len(ncol(Anc_str)))
Anc_str$PC1<-gsub("\\[","",Anc_str$PC1)
Anc_str$PC16<-gsub("\\]","",Anc_str$PC16)

# add family IDs 

pheno_pcs <- cbind(Anc_pca$research_id, Anc_pca$research_id, Anc_str)
names(pheno_pcs)[1]="FID"
names(pheno_pcs)[2]="IID"

# remove related people 

# copy related dataset to local file 
system("gsutil -u $GOOGLE_PROJECT cp gs://fc-aou-datasets-controlled/v7/wgs/short_read/snpindel/aux/relatedness/relatedness.tsv .", intern=T)

# load phenotypes

# Copy the file from current workspace to the bucket
system(paste0("gsutil cp ", AOU_PHENO, " ."), intern=T)


# long_pheno <- aou_pheno %>%
mutate(
  sample_id = person_id,
  cohort = 'AOU'
) %>%
  # In AoU 'id' and 'sample_id' are the same, but in other studies, such as UKB, 'sample_id' can be
  # different from 'id'.
  select(id=person_id, sample_id, cohort, age, age2, sex_at_birth, lipid_type, mg_dl = value_as_number) %>%
  mutate(
    IID = sample_id,
    FID = IID
  )

