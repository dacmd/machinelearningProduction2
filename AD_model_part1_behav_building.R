
##In this model please mention company name, project ID you want to test and later it will ask from which date you want to test the data

### Set working directory
source("~/.setwd.r")
getwd()

####turn unnecessary warning off
options(warn=-1)

#####################################
### Setup spark context
spark_path <- strsplit(system("brew info apache-spark",intern=T)[4],' ')[[1]][1] # Get your spark path
.libPaths(c(file.path(spark_path,"libexec", "R", "lib"), .libPaths())) # Navigate to SparkR folder
########
library(SparkR)
sc <- sparkR.init(master = "local[*]",sparkEnvir = list(spark.driver.memory="2g"))
sqlContext <- sparkRSQL.init(sc)
#######################################
### Setup DB Parameters 
#####
library(data.table)

db_cred <- read.table(file='data/db_credentials2.csv', header=TRUE, sep=',', col.names=c('Key','Value'))
#db_cred <- read.table(file="~/Documents/Production_FM_models/sparkR/db_credentials2.csv",sep=',', col.names=c('Key','Value')) 

db_cred_table <- data.table(db_cred,key='Key')
db_Key=db_cred_table$Key
db_Value=db_cred_table$Value

dbParameters <- setNames(as.list(db_Value), db_Key)

url<-paste0("jdbc:mysql://host:port/dbname?user=username&password=pw&company=company_name&useSSL=false&useUnicode=true&characterEncoding=UTF-8")
url=gsub("host", dbParameters$host, url)
url=gsub("port", dbParameters$port, url)
url=gsub("dbname", dbParameters$db, url)
url=gsub("username", dbParameters$username, url)
url=gsub("pw", dbParameters$password, url)
url=gsub("company_name", dbParameters$company_name, url)
##########################################

stopQuietly <- function(...) {
  blankMsg <- sprintf("\r%s\r", paste(rep(" ", getOption("width")-1L), collapse=" "));
  stop(simpleError(blankMsg));
} # stopQuietly()



##########################################
## COMPANY NAME

## PLEASE ENTER COMPANY NAME FOR ANALYSIS

### query data from db using spark
#companyID <- collect(read.jdbc(url,"(SELECT id FROM companies where name = 'Cmd') as companies"))  ## OK to collect as its a single element. But if it was a group probably not good.
cat("Hello! Let's start execution")
library(tcltk)


cat("To begin execution you have to enter company name and then press enter. Company options available are: Cmd")
companyName=readline(prompt="Enter CompanyName from which you want to test data, enter Cmd for now ")

companyName
companyName=as.character(companyName)

if(companyName=="")
{print(paste0("No company name added, adding my company name, Cmd"))
  companyName=as.character(Cmd)}else
{print(paste0("Company name is added, continuing execution, please be patient, thank you"))}
  

cat("Finding ID for company = Cmd")

companyQuery <- paste0("(SELECT id FROM companies WHERE name in ('", paste(companyName),"')) as companies", sep="")
companyID <- collect(read.jdbc(url, companyQuery))
companyID

## check needed to make sure we have data not sure how to do that, if we wanted a group of values we could do that too but still need to check returned data is more then zero
projectQuery <- paste0("(SELECT id FROM projects WHERE company_id in (", paste(companyID,collapse = ","),")) as projectID", sep="")
projectID_all <- collect(read.jdbc(url, projectQuery))

projectID_all$id ### currently there are 48 entries returned


#### PLEASE SELECT PROJECT ID FOR ANALYSIS
cat("Please select project id from available list:",projectID_all$id)
Project_id=readline(prompt="Enter only 1 Project ID for which you want to test data, id = 1 is recommended for now ")
Project_id=Project_id

projectID=noquote(Project_id)

if(projectID=="")
{print(paste0("No projectID added, adding my projectID, 1"))
  projectID=1}else
  {print(paste0("projectID is added, continuing execution, please be patient, thank you"))}


projectID
############################################
#### SERVER QUERY
cat("Executing server query")

serverQuery<- paste0("(SELECT id FROM servers_project_X WHERE project_id in (", paste(projectID, collapse = ","),")) as servers", sep="")
serverQuery=gsub("X", projectID, serverQuery) ## we shouldn't be doing this
serverDF <- read.jdbc(url, serverQuery)

showDF(serverDF)
count(serverDF)

############################################
#### SESSION QUERY
cat("Executing session query")

sessionQuery <- paste0("(SELECT
                       s.id,
                       s.server_id,
                       s.pid,
                       IFNULL(CAST(AES_DECRYPT(dsrhostname.ears_value,UNHEX('AESkey'))as CHAR),'') as hostname,
                       
                       IFNULL(CAST(AES_DECRYPT(dsrip.ears_value,UNHEX('AESkey'))as CHAR), '') as host_ip,
                       #IFNULL( TRIM(BOTH ',' FROM AES_DECRYPT(dserver_groups.ears_value, UNHEX('AESkey'))), '') as groups,
                       AES_DECRYPT(dsuser.ears_value, UNHEX('AESkey')) as user,
                       u.username as cmd_username,
                       u.name as cmd_fullname,
                       IFNULL(CAST(AES_DECRYPT(dsip.ears_value, UNHEX('AESkey'))as CHAR), '') as ip,
                       s.date_created,
                       s.date_disconnected,
                       s.first_time_seen,
                       i.country_code,
                       i.ip_type,
                       s.session_duration,
                       s.date_last_command,
                       s.commands AS commands_count,
                       s.violations,
                       i.country_name,
                       i.region_code,
                       i.city,
                       i.postal_code,
                       AES_DECRYPT(dslatitude.ears_value, UNHEX('AESkey'))  as latitude,
                       AES_DECRYPT(dslongitude.ears_value, UNHEX('AESkey'))  as longitude,
                       i.continent_code,
                       i.region_name,
                       s.ip_risk_score,
                       sr.project_id,
                       s.triggered_count - s.violations as triggered_count,
                       s.how_connected,
                       IFNULL( AES_DECRYPT(dsports.ears_value, UNHEX('AESkey')) , '') as ports,
                       
                       s.created_ip,
                       IFNULL( AES_DECRYPT(dscreated_by.ears_value, UNHEX('AESkey')) , '') as created_by,
                       #s.shell_ssh,
                       #s.shell_login,
                       s.interactive,
                       s.auth_user_id,
                       s.auth_username as duo_approver,
                       s.time_created,
                       s.stopped_by_user_date
                       
                       
                       FROM
                       
                       
                       sessions_project_0 as s
                       JOIN servers_project_0 as sr ON (s.server_id = sr.id)
                       JOIN projects as p ON (p.id = sr.project_id)
                       JOIN analytics_opt_in as o ON (
                       p.id = o.project_id AND
                       STR_TO_DATE(CONCAT( s.date_created,  MINUTE(s.time_created)), '%Y%m%d%H%i:%s') BETWEEN o.start_date AND if(o.end_date IS NULL, NOW(), o.end_date))
                       
                       LEFT JOIN ips as i ON (s.ip = i.id AND 0 = i.project_id)
                       LEFT JOIN ears_dictionary_project_0  AS dsports ON ( s.ports = dsports.id)
                       
                       LEFT JOIN ears_dictionary_project_0  AS dscreated_by ON ( s.created_by = dscreated_by.id)
                       LEFT JOIN ears_dictionary_project_0  AS dsuser ON ( s.user = dsuser.id)
                       LEFT JOIN ears_dictionary_project_0   AS dslatitude ON ( i.latitude = dslatitude.id)
                       LEFT JOIN ears_dictionary_project_0   AS dslongitude ON ( i.longitude = dslongitude.id)
                       LEFT JOIN ears_dictionary_project_0  AS dsip ON ( s.ip = dsip.id)
                       LEFT JOIN ears_dictionary_project_0  AS dsrip ON ( sr.ip = dsrip.id)
                       LEFT JOIN ears_dictionary_project_0   AS dsrhostname ON ( sr.hostname = dsrhostname.id)
                       LEFT JOIN ears_dictionary_project_0   AS dserver_groups ON ( sr.groups = dserver_groups.id)
                       LEFT JOIN users as u ON (s.auth_user_id = u.id))as session")


###make changes to query

sessionQuery=gsub("_0", paste("_", projectID, sep=""), sessionQuery)

### read AES key credentials based on company id
company_keys <- read.table(file='data/company_keys.csv', header=TRUE, sep=',', col.names=c('company_id','company_key'))

keys_table <- data.table(company_keys, key='company_key')
company_id = keys_table$company_id
company_key = keys_table$company_key

aes_keys_by_company <- setNames(as.list(company_key), company_id)
### change AESkey based on company id in sessionQuery
sessionQuery=gsub("AESkey", aes_keys_by_company$`1`, sessionQuery)


##execute session query
sessionDF <- read.jdbc(url, sessionQuery)
createOrReplaceTempView(sessionDF, "session")

########### verifying if session data is fine
dim_sess=dim(sessionDF)
dim_sess1=dim_sess[1]
dim_sess2=dim_sess[2]


if(dim_sess1==0)
{print(paste0("Rows in session are empty, stopping execution"))
  stopQuietly()}else
    if(dim_sess2==0)
    {print(paste0("Columns in session are empty, stopping execution"))
      stopQuietly()}else
      {print(paste0("Session data is fine, continuing execution"))}

###############################################
cat("Finding unique users")

##### FIND UNIQUE USERS

showDF(sql("select count(distinct(auth_user_id)) from session")) #total users
uni_user=sql("select distinct(auth_user_id) from session")
createOrReplaceTempView(uni_user, "users_out")
showDF(uni_user)

data_user1=sql("select id, substring(date_created, 0,8) as date, auth_user_id, time_created from session where auth_user_id=1")
showDF(data_user1)

##################################################
cat("READING DATA IS COMPLETE, NOW WE BEGIN ANALYSIS")
### READING DATA IS COMPLETE, NOW WE BEGIN ANALYSIS
##################################################
### WE WILL FIRST DERIVE FEATURES
cat("Deriving Features")
##################################################
cat(" Feature 1: SESSIONS/DAY")
## Feature 1: SESSIONS/DAY
############################
## Feature 1: sessions/day
session_per_day=sql("select substring(date_created, 0,8) as date,count(distinct(id)) as sessionPerDay from session where auth_user_id=1 group by date order by date asc")
showDF(session_per_day)

############################
####FIND SESSION IDs FOR COMMAND QUERY

# #### FIND SESSION ID'S
session_id=sql("select distinct(id) as sessionId from session where auth_user_id=1 ")
showDF(session_id)
createOrReplaceTempView(session_id, "session_id")

 id=c(collect(session_id))
 id_all=as.numeric(do.call(rbind.data.frame, id))
 id_all=id_all[order(id_all)]
 id_in=paste(as.character(id_all), collapse=",")

###############################
cat("querrying cmd data")
##### CMD QUERY
cmdQuery <- paste0("(SELECT c.id,
                   c.command_type,
                   c.date_created,
                   c.trigger_status,
                   c.user_typed,
                   c.session_id,
                   #  c.parent_server_cmd_id,
                   c.server_cmd_id,
                   c.extra_vars,
                   c.pid,
                   c.parent_server_cmd_id,
                   c.child_count,
                   # c.command_line_id,
                   c.time_created,
                   c.is_parent,
                   CAST(AES_DECRYPT(dcmd.ears_value,UNHEX('AESkey'))as CHAR) as cmd,
                   CAST(AES_DECRYPT(dcmd_root.ears_value,UNHEX('AESkey'))as CHAR) as cmd_root,

                   AES_DECRYPT(dparams.ears_value,UNHEX('AESkey')) as parameters,
                   CAST(AES_DECRYPT(dcwd.ears_value,UNHEX('AESkey')) as CHAR)as cwd,

                   AES_DECRYPT(dexec_path.ears_value,UNHEX('AESkey')) as exec_path,
                   AES_DECRYPT(dbinary_hash.ears_value,UNHEX('AESkey')) as binary_hash

                   FROM commands_project_0 AS c
                   LEFT JOIN ears_dictionary_project_0 as dcmd ON (c.cmd = dcmd.id)
                   LEFT JOIN ears_dictionary_project_0 as dcmd_root ON (c.cmd_root = dcmd_root.id)
                   LEFT JOIN ears_dictionary_project_0 as dparams ON (c.parameters = dparams.id)
                   LEFT JOIN ears_dictionary_project_0 as dcwd ON (c.cwd = dcwd.id)
                   LEFT JOIN ears_dictionary_project_0 as dbinary_hash ON (c.binary_hash = dbinary_hash.id)
                   LEFT JOIN ears_dictionary_project_0 as dexec_path ON (c.exec_path = dexec_path.id)
                   WHERE
                   c.session_id in (id_in))as cmdOut")


#WHERE c.session_id = 1 )as cmdOut")


###make changes to query

cmdQuery=gsub("_0", paste("_", projectID, sep=""), cmdQuery)
### change AESkey based on company id in sessionQuery

cmdQuery=gsub("AESkey", as.character(aes_keys_by_company$'1'), cmdQuery)

cmdQuery=gsub("id_in", id_in, cmdQuery)

##execute cmd query
cmdDF <- read.jdbc(url, cmdQuery)
createOrReplaceTempView(cmdDF, "cmdOut")

showDF(cmdDF)


#####################################
#### FEATURE 2:CMDS/SESSION
cat("FEATURE 2:CMDS/SESSION")

cmd_per_sess=sql("select session_id as sess_id, count(lower(cmd)) as cmds from cmdOut group by session_id order by session_id asc")
showDF(cmd_per_sess)

sess_in_day=sql("select substring(date_created, 0,8) as date,id as sess_id from session where auth_user_id=1 order by date asc")
showDF(sess_in_day)

cmd_per_sess_per_day=merge(sess_in_day, cmd_per_sess)
showDF(cmd_per_sess_per_day )

##########################################
####### FEATURE 3: SESSION DURATION
cat("FEATURE 3: SESSION DURATION")

durQuery <- paste0("(SELECT id as sess_id,TIMESTAMPDIFF(SECOND, STR_TO_DATE(CONCAT(date_created, ' ', MINUTE(time_created), ' ', SECOND(time_created)), '%Y%m%d%H %i %s'), date_last_command) as time FROM sessions_project_0 where auth_user_id=1)as durOut")
durQuery=gsub("_0", paste("_", projectID, sep=""), durQuery)

durDF <- read.jdbc(url, durQuery)

createOrReplaceTempView(durDF, "durOut")
showDF(durDF)

sess_date=sql("select substring(date_created, 0,8) as date,id as sess_id from session where auth_user_id=1")
showDF(sess_date)

###MERGE DATE WITH SESSION ID AND SESSION DURATION

sess_date_dur=merge(sess_date, durDF,by=c("sess_id"),sort=TRUE)
showDF(sess_date_dur)
# 
####################################### 
### FEATURE 4 AND 5: CMD REP (GOOD, BAD)

cat("FEATURE 4 AND 5: CMD REP (GOOD, BAD)")
###load required libraries
library(NLP)
library(tm)
#data training

trainingDF <- read.df("cmd_rep_train_4.csv", "csv", header = "true", inferSchema = "true")
showDF(trainingDF)

# #cmds only
cmd_user=sql("select session_id as sess_id, lower(cmd) as cmd_test from cmdOut")
# #showDF(cmd_user)
cmd_user_test=collect(cmd_user)
dim_cmd_test=dim(cmd_user_test)
len_cmd_test=dim_cmd_test[1]
############  Sanitization of data in R
### convert sparkr dataframe to r dataframe for sanitization
training_r_df=collect(trainingDF)
training_all_cmd_data=training_r_df$cmd
test_all_cmd_data=cmd_user_test$cmd_test
all_cmd_data=c(training_all_cmd_data,test_all_cmd_data)

len_train=length(training_all_cmd_data)
len_test=length(test_all_cmd_data)
len_all=length(all_cmd_data)

sanitized_data = Corpus(VectorSource(all_cmd_data))
sanitized_data = tm_map(sanitized_data, content_transformer(tolower))
sanitized_data = tm_map(sanitized_data, removeNumbers)
sanitized_data =  tm_map(sanitized_data, stripWhitespace)

##To analyze the textual data, we use a Document-Term Matrix (DTM) representation: documents as the rows, terms/words as the columns,
##frequency of the term in the document as the entries.
dtm <- DocumentTermMatrix(sanitized_data)
dtm_matrix<-as.matrix(dtm)
##apply PCA
library(caret)
pr1<-prcomp(dtm_matrix, scale = TRUE)
newdat<-data.frame(pr1$x[,1:60])

#dtm_matrix train
dtm_label_df=data.frame(cbind(training_r_df$label,newdat[1:len_train,])) ##combining label with data
dtm_label_named=setnames(dtm_label_df[1], "training_r_df.label", "label") ##renaming label column
dtm_label_df=cbind(dtm_label_named,newdat[1:len_train,]) ##combining renamed label with data


#dtm_matrix test
dtm_label_df_test=data.frame(newdat[(len_train+1):len_all,]) ##combining label with data


####### Convert R dataframe back to spark dataframe for Classification
cleaned_trainingDF <-createDataFrame(sqlContext, dtm_label_df)
showDF(cleaned_trainingDF)

cleaned_testDF <-createDataFrame(sqlContext, dtm_label_df_test)

####### Classification using RF
model1 <- spark.randomForest(cleaned_trainingDF, label ~ ., type="classification", seed=123456,numTrees = 100)
predictions_valid <- predict(model1, cleaned_trainingDF)
predictions_test <- predict(model1, cleaned_testDF)

##### Results
prediction_df_valid <- collect(select(predictions_valid, "label", "prediction")) ##extracting results from spark
confMat <- table(prediction_df_valid[,1],prediction_df_valid[,2]) # comparing results with original labels
confMat ##confusion matrix
accuracy <- sum(diag(confMat))/sum(confMat) #calculating accuracy
accuracy

prediction_df_test <- collect(select(predictions_test, "probability", "prediction")) ##extracting results from spark

# #to read probability is spark data frame
# modelPrediction <- select(predictions_test, "probability")
# localPredictions <- SparkR:::as.data.frame(predictions_test)
#
# getValFrmDenseVector <- function(x) {
#   #Given it's binary classification there are just two elems in vector
#   a <- SparkR:::callJMethod(x$probability, "apply", as.integer(0))
#   b <- SparkR:::callJMethod(x$probability, "apply", as.integer(1))
#   c(a, b)
# }
#
# prob_test=t(apply(localPredictions, 1, FUN=getValFrmDenseVector))#probability
modelPrediction_res <- select(predictions_test, "prediction")#predictions

#cmd_rep_result

cmd_rep_match_val=modelPrediction_res
sess_cmd_rep=cbind(cmd_user_test,prediction_df_test[,2])
###########################################
### FEATURE 6: CMD MATCH SCORE
cat("FEATURE 6: CMD MATCH SCORE")
cmd_per_sess_day=sql("select session_id as sess_id, lower(cmd) as cmds from cmdOut")
showDF(cmd_per_sess_day)
createOrReplaceTempView(cmd_per_sess_day,"cmd_sess")
# #
cmd_for_match=collect(cmd_per_sess_day)
agg_cmd=aggregate(cmds ~ sess_id, data = cmd_for_match, c)
sess_date_r=collect(sess_date)
tot_cmd_sess=cbind(sess_date_r,agg_cmd)

dim_tot_cmd=dim(tot_cmd_sess)
len_tot_cmd=dim_tot_cmd[1]
#
uniq_date=unique(tot_cmd_sess[,1])
cmd_sess_match_score=matrix('',nrow=length(uniq_date),ncol=10)
library(RecordLinkage)

############ this code matches all commands across days (day 1 with day 2, day 2 with day 3 and so on)
#
cmd_day_match_score=matrix('',nrow=length(uniq_date),ncol=10)
cmd_day_match_score_out=vector('numeric',length=length(uniq_date))

data_date_sess=sql("select substring(date_created, 0,8) as date, id as sess_id from session where auth_user_id=1")
agg_cmd2=createDataFrame( sqlContext, agg_cmd)
all_data=merge(data_date_sess,agg_cmd2)
createOrReplaceTempView(all_data, "datecommand")

dateCommand=sql("select date as date, cmds as cmd from datecommand ")#group by date")
createOrReplaceTempView(dateCommand,"dateCommandOut")

dateCommandData=collect(dateCommand)
dateCommandDataAll=aggregate(as.character(cmd) ~ date ,data = dateCommandData, c)

no_of_day_data=length(as.numeric(uniq_date)) #no. of days considered

data_Match_score=vector('numeric',length=no_of_day_data)
for(i in 2:no_of_day_data)
{data1_toMatch=as.character(data.frame(matrix(unlist(dateCommandDataAll[i,2]), nrow=length(unlist(tot_cmd_sess[i,2])), byrow=T),stringsAsFactors=FALSE))
data2_toMatch=as.character(data.frame(matrix(unlist(dateCommandDataAll[i-1,2]), nrow=length(unlist(tot_cmd_sess[i-1,2])), byrow=T),stringsAsFactors=FALSE))
data_Match_score[i]=levenshteinSim(as.character(data1_toMatch),as.character(data2_toMatch))}

data_Match_score[1]=1
# 
################################################################
###############################################################
####We decide conditions here
##First we see how many days of data we have
###In future we will break the code from here into part two where daily data will be loaded and behaviour will be calculated.
cat("We now decide conditions for all 6 features for AD model")

no_of_day_data
dates_data_available=collect(data_user1)

###select for which date you want to test the data, available dates of data is

cat("Data is available for following dates",unique(dates_data_available$date))
date_test <- readline(prompt="Enter date from which you want to test data ")
date_for_test=date_test
date_for_test

day_for_testing_match=which(as.character(unique(dates_data_available$date))==date_for_test)

if(day_for_testing_match==1)
{train_day=1:1
test_day=day_for_testing_match}else
{train_day=1:(day_for_testing_match-1)
test_day=day_for_testing_match:(no_of_day_data)
}
#################################
##condition 1: sessions/day

session_data=collect(session_per_day)
#dim_sess_data=dim(session_data)
#len_sess_data=dim_sess_data[1]
##################LETS CALCULATE DATES FOR PREVIOUS DAYS, WEEEK, MONTH
library(lubridate)
date_test <- as.Date(session_data$date[test_day], format = "%Y%m%d") 
#previous_day <- floor_date(date_test[1]-1, "day") #round to nearest previous day

dates=as.Date(session_data$date, format = "%Y%m%d") 

previous_day_org <-seq(date_test[1]-1,date_test[1],by='day')[1]
diff_day=difftime(dates,previous_day_org,units = 'days' )
diff_day_ind=which(difftime(dates,previous_day_org,units = 'days' )==1)
#prev_day_ind=which(diff_day>0&diff_day<1)
previous_day=dates[diff_day_ind-1]

previous_week_org <-seq(date_test[1]-7,date_test[1],by='day')[1]
diff_week=difftime(dates,previous_week_org,units = 'day' )
diff_week_ind=which(difftime(dates,previous_week_org,units = 'day' )==7)
prev_week_ind=min(which(diff_week>0&diff_week<7))
if(dates[prev_week_ind]!=previous_day)
{previous_week=(dates[prev_week_ind])}else
{previous_week=(dates[prev_week_ind-1])}


previous_month_org <-seq(date_test[1]-30,date_test[1],by='day')[1]
diff_month=difftime(dates,previous_month_org,units = 'day' )
diff_month_ind=which(difftime(dates,previous_month_org,units = 'day' )==30)
prev_month_ind=min(which(diff_month>0&diff_month<30))
if(dates[prev_month_ind]!=previous_week)
{previous_month=(dates[prev_month_ind])}else
{previous_month=(dates[prev_month_ind-1])}


###################################
###NOW FIND CONDITIONS

##condition 1: sessions/day

#if session/day for test data >150 % sessions/day of previous day, 
# > 140 % sessions/day of same day of previous day, and 
# > 130% sessions/day over last month, then session/day = AD

#for single session
#session_data_testDay=session_data$sessionPerDay[which(as.Date(session_data$date, format = "%Y%m%d")==date_test)]

#for multiple session
session_data_testDay=c(session_data$session[which(as.Date(session_data$date, format = "%Y%m%d")==date_test[1]):which(as.Date(session_data$date, format = "%Y%m%d")==date_test[length(date_test)])])
session_data_trainDay=session_data$sessionPerDay[which(as.Date(session_data$date, format = "%Y%m%d")==previous_day)]
#session_data_trainWeek=session_data$sessionPerDay[which(as.Date(session_data$date, format = "%Y%m%d")==previous_week)]
#session_data_trainMonth=session_data$sessionPerDay[which(as.Date(session_data$date, format = "%Y%m%d")==previous_month)]#
all_session_data_trainWeek= c(session_data$session[which(as.Date(session_data$date, format = "%Y%m%d")==previous_week):which(as.Date(session_data$date, format = "%Y%m%d")==date_test[1])])
avg_session_data_trainWeek=mean( all_session_data_trainWeek[length( all_session_data_trainWeek)-1]) # as last session is test session
avg_session_data_trainMonth= mean(c(session_data$session[which(as.Date(session_data$date, format = "%Y%m%d")==previous_week):which(as.Date(session_data$date, format = "%Y%m%d")==previous_month)]))

session_data_out=vector('numeric',length=length(test_day))

for(i in 1:length(session_data_testDay))
{if(previous_day!=0) #if train day is 0 we will consider weekly and monthly data
{ if(session_data_testDay[i]>1.5* session_data_trainDay&session_data_testDay[i]>1.4* avg_session_data_trainWeek&session_data_testDay[i]>1.3* avg_session_data_trainMonth)
{session_data_out[test_day[i]]=1}else
{session_data_out[test_day[i]]=0}
}else
{if(session_data_testDay[i]>1.4* avg_session_data_trainWeek&session_data_testDay[i]>1.3* avg_session_data_trainMonth)
{session_data_out[test_day[i]]=1}else
{session_data_out[test_day[i]]=0}
}
}

session_data_out[test_day]
###################################
####condition 2 cmds/day
##If average number of cmds in a day >150% of average of cmds from previous day AND 
## > 140% of average of cmds from previous week AND > 130% of average of cmds from previous month then value = 1 else 0.

cmd_data_PerDay=collect(cmd_per_sess_per_day )
cmd_data_agg_PerDay=aggregate(cmds ~ date, data = cmd_data_PerDay, FUN = mean)
cmd_data_test <- as.Date(cmd_data_agg_PerDay$date[test_day], format = "%Y%m%d") 

#single day
##cmd_data_testDay=cmd_data_agg_PerDay$cmds[which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==date_test)]

##multiple days
cmd_data_testDay=c(cmd_data_agg_PerDay$cmds[which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==date_test[1]):which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==date_test[length(date_test)])])

cmd_data_trainDay=cmd_data_agg_PerDay$cmds[which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==previous_day)]

all_cmd_data_trainWeek= c(cmd_data_agg_PerDay$cmds[which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==previous_week):which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==date_test[1])])
avg_cmd_data_trainWeek=mean( all_cmd_data_trainWeek[length( all_cmd_data_trainWeek)-1]) # as last day is test session
avg_cmd_data_trainMonth= mean(c(cmd_data_agg_PerDay$cmds[which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==previous_week):which(as.Date(cmd_data_agg_PerDay$date, format = "%Y%m%d")==previous_month)]))

cmd_data_out=vector('numeric',length=length(test_day))

for(i in 1:length(test_day))
{if(previous_day!=0) #if train day is 0 we will consider weekly and monthly data
{ if(cmd_data_testDay[i]>1.5* cmd_data_trainDay&cmd_data_testDay[i]>1.4* avg_cmd_data_trainWeek&cmd_data_testDay[i]>1.3* avg_cmd_data_trainMonth)
{cmd_data_out[test_day[i]]=1}else
{cmd_data_out[test_day[i]]=0}
}else
{if(cmd_data_testDay[i]>1.4* avg_cmd_data_trainWeek&cmd_data_testDay[i]>1.3* avg_cmd_data_trainMonth)
{cmd_data_out[test_day[i]]=1}else
{cmd_data_out[test_day[i]]=0}
}
}

cmd_data_out[test_day]
#####################################
####condition 3 Duration/day
dur_data_PerDay=collect(sess_date_dur )
dur_data_agg_PerDay=aggregate(time ~ date, data = dur_data_PerDay, FUN = mean)
dur_data_test <- as.Date(dur_data_agg_PerDay$date[test_day], format = "%Y%m%d") 

#dur_data_testDay=dur_data_agg_PerDay$time[which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==date_test)]

dur_data_testDay=c(dur_data_agg_PerDay$time[which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==date_test[1]):which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==date_test[length(date_test)])])

dur_data_trainDay=dur_data_agg_PerDay$time[which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==previous_day)]

all_dur_data_trainWeek= c(dur_data_agg_PerDay$time[which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==previous_week):which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==date_test[1])])
avg_dur_data_trainWeek=mean( all_dur_data_trainWeek[length( all_dur_data_trainWeek)-1]) # as last day is test session
avg_dur_data_trainMonth= mean(c(dur_data_agg_PerDay$time[which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==previous_week):which(as.Date(dur_data_agg_PerDay$date, format = "%Y%m%d")==previous_month)]))

dur_data_out=vector('numeric',length=length(test_day))
for(i in 1:length(test_day))
{if(previous_day!=0) #if train day is 0 we will consider weekly and monthly data
{ if(dur_data_testDay[i]>1.5* dur_data_trainDay&dur_data_testDay[i]>1.4* avg_dur_data_trainWeek&dur_data_testDay[i]>1.3* avg_dur_data_trainMonth)
{dur_data_out[test_day[i]]=1}else
{dur_data_out[test_day[i]]=0}
}else
{if(dur_data_testDay[i]>1.4* avg_dur_data_trainWeek&dur_data_testDay[i]>1.3* avg_dur_data_trainMonth)
{dur_data_out[test_day[i]]=1}else
{dur_data_out[test_day[i]]=0}
}
}
dur_data_out[test_day]
###########################################
###condition 4 and 5 good_cmd/day

#### good and bad commands for day are calculated as relative to total commands in a day
#sess_cmd_rep=cbind(cmd_user_test,prediction_df_test[,2])
head(sess_cmd_rep)
names(sess_cmd_rep)=c("sess_id","cmd_test","cmd_rep")
head(sess_cmd_rep)
head(cmd_data_PerDay)
dim_sess_cmd=dim(sess_cmd_rep)
len_sess_cmd=dim_sess_cmd[1]
uni_sess_id=unique(sess_cmd_rep$sess_id)

good_sess_rep=vector('numeric',length=length(uni_sess_id))
bad_sess_rep=vector('numeric',length=length(uni_sess_id))
tot_cmd_sess=vector('numeric',length=length(uni_sess_id))

for(i in 1:length(unique(sess_cmd_rep$sess_id)))
{temp=which(sess_cmd_rep$sess_id==uni_sess_id[i])
good_sess_rep[i]=length(which(sess_cmd_rep$cmd_rep[temp]=="GOOD"))
bad_sess_rep[i]=length(which(sess_cmd_rep$cmd_rep[temp]=="BAD"))
tot_cmd_sess[i]=length(temp)
}

sess_cmd_rep_tot=cbind(uni_sess_id,good_sess_rep,bad_sess_rep,tot_cmd_sess)
cmd_rep_sess_day=data.frame(cbind(cmd_data_PerDay$date,cmd_data_PerDay$sess_id_x,cmd_data_PerDay$cmds,sess_cmd_rep_tot[,2],sess_cmd_rep_tot[,3]))
colnames(cmd_rep_sess_day)=c("date","sess_id","tot_cmd","good_cmd","bad_cmd")

cmd_rep_tot_agg_PerDay=aggregate(as.numeric(as.vector(cmd_rep_sess_day[,3])) ~ date, data = cmd_rep_sess_day, FUN = mean)
cmd_rep_good_agg_PerDay=aggregate(as.numeric(as.vector(cmd_rep_sess_day[,4])) ~ date, data = cmd_rep_sess_day, FUN = mean)
cmd_rep_bad_agg_PerDay=aggregate(as.numeric(as.vector(cmd_rep_sess_day[,5])) ~ date, data = cmd_rep_sess_day, FUN = mean)

cmd_rep_data_agg_PerDay=cbind(as.character(unique(cmd_rep_sess_day$date)),session_data$sessionPerDay,cmd_rep_tot_agg_PerDay[,2],cmd_rep_good_agg_PerDay[,2],cmd_rep_bad_agg_PerDay[,2])
colnames(cmd_rep_data_agg_PerDay)=c("date","sess_id","tot_cmd","good_cmd","bad_cmd")
tail(cmd_rep_data_agg_PerDay)

cmd_rep_relative_data_agg_PerDay=cbind(as.character(unique(cmd_rep_sess_day$date)),session_data$sessionPerDay,cmd_rep_data_agg_PerDay[,3],(cmd_rep_good_agg_PerDay[,2]/cmd_rep_tot_agg_PerDay[,2]),(cmd_rep_bad_agg_PerDay[,2]/cmd_rep_tot_agg_PerDay[,2]))
colnames(cmd_rep_relative_data_agg_PerDay)=c("date","sess_id","tot_cmd","rel_good_cmd","rel_bad_cmd")
tail(cmd_rep_relative_data_agg_PerDay)

###good data conditions
cmd_rep_good_data_test <- as.Date(cmd_data_agg_PerDay$date[test_day], format = "%Y%m%d") #test date
# 
#cmd_good_data_testDay=cmd_rep_relative_data_agg_PerDay[,4][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test)]
cmd_good_data_testDay=c(cmd_rep_relative_data_agg_PerDay[,4][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test[1]):which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test[length(date_test)])])


cmd_good_data_trainDay=cmd_rep_relative_data_agg_PerDay[,4][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_day)]
# 
all_cmd_good_data_trainWeek= c(cmd_rep_relative_data_agg_PerDay[,4][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_week):which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test[1])])
avg_cmd_good_data_trainWeek=mean(as.numeric(all_cmd_good_data_trainWeek[length( all_cmd_good_data_trainWeek)-1])) # as last day is test session
avg_cmd_good_data_trainMonth= mean(as.numeric(c(cmd_rep_relative_data_agg_PerDay[,4][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_week):which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_month)])))
# 
cmd_good_data_out=vector('numeric',length=length(test_day))
for(i in 1:length(test_day))
{if(previous_day!=0) #if train day is 0 we will consider weekly and monthly data
{ if(as.numeric(cmd_good_data_testDay[i])<0.5* as.numeric(cmd_good_data_trainDay)&as.numeric(cmd_good_data_testDay[i])<0.4* as.numeric(avg_cmd_good_data_trainWeek)&as.numeric(cmd_good_data_testDay[i])<0.3* as.numeric(avg_cmd_good_data_trainMonth))
{cmd_good_data_out[test_day[i]]=1}else
{cmd_good_data_out[test_day[i]]=0}
}else
{if(as.numeric(cmd_good_data_testDay[i])<0.4* as.numeric(avg_cmd_good_data_trainWeek)&as.numeric(cmd_good_data_testDay[i])<0.3* as.numeric(avg_cmd_good_data_trainMonth))
{cmd_good_data_out[test_day[i]]=1}else
{cmd_good_data_out[test_day[i]]=0}
}
}
cmd_good_data_out[test_day]
### Bad data conditions
cmd_rep_bad_data_test <- as.Date(cmd_data_agg_PerDay$date[test_day], format = "%Y%m%d") #test date
# 
#cmd_bad_data_testDay=cmd_rep_relative_data_agg_PerDay[,5][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test)]
cmd_bad_data_testDay=c(cmd_rep_relative_data_agg_PerDay[,5][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test[1]):which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test[length(date_test)])])

cmd_bad_data_trainDay=cmd_rep_relative_data_agg_PerDay[,5][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_day)]
# 
all_cmd_bad_data_trainWeek= c(cmd_rep_relative_data_agg_PerDay[,5][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_week):which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==date_test[1])])
avg_cmd_bad_data_trainWeek=mean(as.numeric(all_cmd_bad_data_trainWeek[length( all_cmd_bad_data_trainWeek)-1])) # as last day is test session
avg_cmd_bad_data_trainMonth= mean(as.numeric(c(cmd_rep_relative_data_agg_PerDay[,5][which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_week):which(as.Date(cmd_rep_relative_data_agg_PerDay[,1], format = "%Y%m%d")==previous_month)])))
# 
cmd_bad_data_out=vector('numeric',length=length(test_day))
for(i in 1:length(test_day))
{if(previous_day!=0) #if train day is 0 we will consider weekly and monthly data
{ if(as.numeric(cmd_bad_data_testDay[i])>1.5* as.numeric(cmd_bad_data_trainDay)&as.numeric(cmd_bad_data_testDay[i])>1.4* as.numeric(avg_cmd_bad_data_trainWeek)&as.numeric(cmd_bad_data_testDay[i])>as.numeric(1.3* avg_cmd_bad_data_trainMonth))
{cmd_bad_data_out[test_day[i]]=1}else
{cmd_bad_data_out[test_day[i]]=0}
}else
{if(as.numeric(cmd_bad_data_testDay[i])>1.4* as.numeric(avg_cmd_bad_data_trainWeek)&as.numeric(cmd_bad_data_testDay[i])>1.3* as.numeric(avg_cmd_bad_data_trainMonth))
{cmd_bad_data_out[test_day[i]]=1}else
{cmd_bad_data_out[test_day[i]]=0}
}
}
######################
######################
####condition 6 cmd_match_score/day
cmd_day_match_score_out=data_Match_score
session_data=collect(session_per_day)
cmd_match_score_data=cbind(session_data,cmd_day_match_score_out) #combined dates, session, cmd_match_score
# 
#cmd_match_score_data_testDay=cmd_match_score_data$cmd_day_match_score_out[which(as.Date(session_data$date, format = "%Y%m%d")==date_test)]
cmd_match_score_data_testDay=c(cmd_match_score_data$cmd_day_match_score_out[which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==date_test[1]):which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==date_test[length(date_test)])])

cmd_match_score_data_trainDay=cmd_match_score_data$cmd_day_match_score_out[which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==previous_day)]
all_cmd_match_score_data_trainWeek= c(cmd_match_score_data$cmd_day_match_score_out[which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==previous_week):which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==date_test[1])])
avg_cmd_match_score_data_trainWeek=mean( all_cmd_match_score_data_trainWeek[length( all_cmd_match_score_data_trainWeek)-1]) # as last session is test session
avg_cmd_match_score_data_trainMonth= mean(c(cmd_match_score_data$cmd_day_match_score_out[which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==previous_week):which(as.Date(cmd_match_score_data$date, format = "%Y%m%d")==previous_month)]))
# 
cmd_match_score_data_out=vector('numeric',length=length(test_day))
for(i in 1:length(test_day))
{
if(previous_day!=0) #if train day is 0 we will consider weekly and monthly data
{ if(cmd_match_score_data_testDay[i]<0.5* cmd_match_score_data_trainDay&cmd_match_score_data_testDay[i]<0.4* avg_cmd_match_score_data_trainWeek&cmd_match_score_data_testDay[i]<0.3* avg_cmd_match_score_data_trainMonth)
{cmd_match_score_data_out[test_day[i]]=1}else
{cmd_match_score_data_out[test_day[i]]=0}
}else
{if(cmd_match_score_data_testDay[i]<0.4* avg_cmd_match_score_data_trainWeek&cmd_match_score_data_testDay[i]<0.3* avg_cmd_match_score_data_trainMonth)
{cmd_match_score_data_out[test_day[i]]=1}else
{cmd_match_score_data_out[test_day[i]]=0}
}
}
####################

####################
## building final part here
####################
cat("Building final AD part")
###############################################
cmd_match_score_data_out[test_day]
cmd_bad_data_out[test_day]
cmd_good_data_out[test_day]
dur_data_out[test_day]
cmd_data_out[test_day]
session_data_out[test_day]
###################
test_data_all=cbind(session_data_out[test_day],cmd_data_out[test_day],cmd_good_data_out[test_day],cmd_bad_data_out[test_day],cmd_match_score_data_out[test_day],dur_data_out[test_day])
colnames(test_data_all) = c("sessions_per_day","cmd_per_day","good_cmd","bad_cmd","five_cmd_match","sess_dur")

####################################################sess_ind
AD_model <- read.csv(file="~/Documents/Production_FM_models/AD_model/train_data_AD.csv",header=FALSE,stringsAsFactors=FALSE)
head(AD_model)
AD_model_dim=dim(AD_model)
AD_model_dim_len=AD_model_dim[1]
AD_model_dim_wid=AD_model_dim[2]
AD_model_rev=AD_model[2:AD_model_dim_len,]
AD_model_var=AD_model_rev[,1:6]
AD_model_class=AD_model_rev[,7]
colnames(AD_model_var) = c("sessions_per_day","cmd_per_day","good_cmd","bad_cmd","five_cmd_match","sess_dur")

#### ML model
train_class=factor(AD_model_class)
train_data3=cbind(AD_model_var,AD_model_class)

# library(randomForest)
# user_rf <- randomForest(AD_model_class~.,data=train_data3,ntree=100,proximity=TRUE,importance=TRUE,seed=1235)
# pred_val=predict(user_rf)
# table(pred_val,train_data3$AD_model_class)
# #################
####### Convert R dataframe back to spark dataframe for Classification
cleaned_trainingDF <-createDataFrame(sqlContext, train_data3)
showDF(cleaned_trainingDF)

####### Classification using RF
model1 <- spark.randomForest(cleaned_trainingDF, AD_model_class ~ ., type="classification", seed=123456,numTrees = 100)
predictions_train_result <- predict(model1, cleaned_trainingDF)  

##### Results 
##validation
cat("validation results")
prediction_df <- collect(select(predictions_train_result, "AD_model_class", "prediction")) ##extracting results from spark
confMat <- table(prediction_df[,1],prediction_df[,2]) # comparing results with original labels
confMat ##confusion matrix
accuracy <- sum(diag(confMat))/sum(confMat) #calculating accuracy
accuracy
#################
##test
cat("test results")
test_data_real=rbind(train_data3[,1:6],test_data_all)
cleaned_testDF <-createDataFrame(sqlContext, test_data_real)
showDF(cleaned_testDF)


predictions_test_result <- predict(model1, cleaned_testDF)  
final_result=collect(predictions_test_result)
out_val=final_result$prediction

dim_all_train_test_data=dim(train_data3)
test_data_len=length(out_val)-dim_all_train_test_data[1]

final_result_out=out_val[(dim_all_train_test_data[1]+1):length(out_val)]
final_result_out
# ##################

