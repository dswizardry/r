# r
R Files

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Arguments

args = commandArgs(trailingOnly=TRUE)

if (length(args)==0) {
  stop("At least two arguments must be supplied", call.=FALSE)
} else if (length(args)==1) {
  stop("At least two arguments must be supplied", call.=FALSE)
} else if (length(args)==2) {
  stop("At least two arguments must be supplied", call.=FALSE)
} else if (length(args)==4) {
  print("Arguments used are correct, nice job", call.=FALSE)
}

sql_filename = args[1]     # sql_filename = /nfs/ETL/temp/Paden.Kosoff/ltv3_campaign.sql
w_day = args[2]            # w_day = /nfs/ETL/temp/Paden.Kosoff/ltv_sunday.csv
model_name  = args[3]      # model_name  = 24
days_remaining  = args[4]  # days_remaining  = 21

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin = Sys.time()


model_name = as.numeric(model_name)
days_remaining = as.numeric(days_remaining)
day24 = substr(w_day, 32, nchar(w_day)-4)


source('/nfs/ETL/temp/Paden.Kosoff/ltv3_func.R')
library(DBI)
library("RPostgreSQL")
library(plyr)
source('/nfs/ETL/temp/Paden.Kosoff/connections.R')

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import the data

drv = dbDriver("PostgreSQL")
print("Importing the data")
end = Sys.time()
print(end-begin)
connect = dbConnect(drv, user=user_id, password=user_pwd, dbname=dbname, host=hostdb, port=portdb)
input_query = readLines(sql_filename)
ltv_dataset = dbGetQuery(connect, input_query)
print("Data has been imported")
end = Sys.time()
print(end-begin)

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Prep the data

weekday = read.csv(w_day)
ltv_dataset = ltv_dataset[,c(1,2,27,19:26,3:18)]
ltv_dataset$period_hour = as.numeric(ltv_dataset$period_hour)
ltv_dataset$clicks = as.numeric(ltv_dataset$clicks)
ltv_dataset$searches = as.numeric(ltv_dataset$searches)
dl_time = ltv_dataset$download_time
cohort_time = gsub("[: -]", "" , dl_time, perl=TRUE)
ltv_dataset = cbind(ltv_dataset, cohort_time)
print("Download time transformed ... i.e. 2016-09-18 21:24:12 ==> 20160918212412")
end = Sys.time()
print(end-begin)
ltv_dataset$cohort_time = as.character(ltv_dataset$cohort_time)
ltv_dataset$dl_hour = substr(ltv_dataset$cohort_time, 1, nchar(ltv_dataset$cohort_time)-4)
ltv_dataset$dl_hour = as.numeric(ltv_dataset$dl_hour)
ltv_dataset$dl_day = substr(ltv_dataset$cohort_time, 1, nchar(ltv_dataset$cohort_time)-6)
ltv_dataset$dl_day = as.numeric(ltv_dataset$dl_day)
ltv_dataset$prid = paste(ltv_dataset$campaign, ltv_dataset$cobrand, sep="_")
ltv_dataset$searchers = ifelse(ltv_dataset$searches == 0, 0, 1)
cobrand = ltv_dataset
prid = cobrand[1,"prid"]
print("Data has been prepped")
end = Sys.time()
print(end-begin)
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Predict Cumulative Clicks

print("Starting predictions")

for (f in 1:nrow(weekday)) {

start = weekday[f,2]
  hours = data.frame("hour" = c(0:23))
  hours$w_start = hours$hour + start
  hours$w_start = as.numeric(hours$w_start)
  col_name = data.frame("num" = 1:100)
  col_name$name = "power"
  col_name$colname = paste(col_name$name, col_name$num, sep="")
  col_name$colname = gsub(" ","", col_name$colname)
  error = matrix(data = NA, nrow = 100, ncol = 24)
  rownames(error) = col_name$colname
  large_file = matrix(data = NA, nrow = 24*(24*days_remaining), ncol = 102)
  large_file = as.data.frame(large_file)
  large_file2 = matrix(data = NA, nrow = 24, ncol = 101)
  large_file2 = as.data.frame(large_file2)
  large_file3 = large_file2
  
  error_result = ltv_prediction_error(hours, cobrand, model_name, days_remaining, day24)
}
end = Sys.time()
end - begin

#Rscript --vanilla /nfs/ETL/temp/Paden.Kosoff/ltv3_import_prep.R /nfs/ETL/temp/Paden.Kosoff/ltv3_campaign.sql /nfs/ETL/temp/Paden.Kosoff/ltv_sunday.csv 24 21
#sunday = matrix(data = c("2016091800","2016092500","2016100200","2016100900","2016101600","2016102300","2016103000"), ncol = 1, nrow = 7)
#write.csv(sunday, "/nfs/ETL/temp/Paden.Kosoff/ltv_sunday.csv")



