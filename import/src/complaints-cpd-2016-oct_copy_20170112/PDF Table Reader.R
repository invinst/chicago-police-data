library(pdftools)
pdf_loc = "/Users/thudson/Documents/Invisible Institute/Data Cleanup Project 20170113/data/1_raw_data/complaints-cpd-2016-oct copy 20170112/foia 14-3668 officers with cr allegations.pdf"

doc = pdf_text(pdf_loc)


## Function handles all the string splitting to create table - is heavily dependent on table format
row_creator<-function(this_row)
{
  ## remove extra spaces for ease of labeling
  this_row <- gsub("\\s+"," ",this_row)
  ## Do a search for dates in string based on format
  pattern1<-'[0-9][0-9] [A-Z][a-z][a-z] [0-9][0-9][0-9][0-9]'
  pattern2<-'[0-9][0-9]-[A-Z][a-z][a-z]-[0-9][0-9][0-9][0-9]'
  ## Begin unsplitting into columns
  this_row<- unlist(strsplit(this_row," Initial "))
  row_values<-this_row[1]
  split_date1 <- unlist(gregexpr(pattern1,this_row[2]))
  split_date2 <- unlist(gregexpr(pattern2,this_row[2]))
  row_values <- append(row_values,substring(this_row[2],1,3))
  row_values <- append(row_values,trimws(substring(this_row[2],4,(split_date1[1]-1)),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],split_date1[1],(split_date1[2]-1)),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],split_date1[2],(split_date2[1]-1)),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],split_date2[1],(split_date2[1]+11)),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],(split_date2[1]+12),(split_date2[1]+14)),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],(split_date2[1]+15),regexpr(" Final ",this_row[2])),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],regexpr(" Final ",this_row[2])+7,regexpr(" Final ",this_row[2])+10),which="both"))
  row_values <- append(row_values,trimws(substring(this_row[2],regexpr(" Final ",this_row[2])+11),which="both"))
  return(row_values)  
}

## create very large empty list to write into
new_table<- vector("list",1000000)

## just keeping an eye on things
page_counter <- 1
counter <- 1
officer_counter <- 1

for(page in doc)
{
  step1 <- strsplit(page,"\n")
  ## split columns on two or more spaces - doesn't catch them all unfortunately
  ##step2 <- lapply(step1[[1]],strsplit,split="\\s{2,}")
  step2 <- step1[[1]]
  ## Needed to remove the text on the first page, 
  if(page_counter==1)
  {
    step2 <- step2[seq(11,(length(step2)-1))]    
  }
  ## Cleans up step2
  for(i in 1:length(step2))
  {
    this_row<-trimws(step2[[i]],which="both")
    
    if(grepl('CR#',this_row) | grepl("Closed Date",this_row) | grepl("Page",this_row) )
    {
      ##print("Header")
    } else if(grepl("Initial",this_row))
    {
      new_row<-this_row
      
    } else if(grepl("Final",this_row))
    {
      new_row<-paste(new_row,this_row,sep=" ")
      new_row<-row_creator(new_row)
      new_row<-append(new_row,officer_name)
      new_table[[counter]]<-new_row
      counter <- counter+1
    } else if(grepl("Date of Appt:",this_row))
    {
      ## unique special case:
      officer_name <- trimws(unlist(strsplit(this_row,"Date of Appt:")),which="both")
      ## to split on first/last name, there is a special case
      if(grepl("WIECHERT",officer_name[1]))
      {
        officer_name[1]<- gsub("JR., PAUL","PAUL JR.",officer_name[1])
      }
      officer_name <- c(trimws(unlist(strsplit(officer_name[1],",")),which="both"),officer_name[2])
      officer_counter<- officer_counter + 1
    }
  }
  page_counter = page_counter+1
}

## Removing Nulls and converting to data-frame
smaller_table<-new_table[!sapply(new_table, is.null)]

## test for special cases
smaller_table[sapply(smaller_table,length)!=13]

col_names = c("CRID","Initial_Complaint_Category","Initial_Complaint_Description",
              "Incident_Date","Complaint_Date","Closed_Date",
              "Final_Finding","Action_Taken",
              "Final_Complaint_Category","Final_Complaint_Description",
              "Officer_Last_Name","Officer_First_Name","Appointment_Date")

new_df <-as.data.frame(t(matrix(unlist(smaller_table),nrow=length(col_names))))
colnames(new_df)<-col_names
View(new_df)

setwd("/Users/thudson/Documents/Invisible Institute/Data Cleanup Project 20170113/data/2_tabled_data/complaints-cpd-2016-oct copy 20170112/")
write.table(new_df,file="foia 14-3668 officers with cr allegations.txt",sep="\t")

