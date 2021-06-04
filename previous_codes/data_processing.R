setwd("/Volumes/files/group_folders/HAICHAO/wendy/clinical_information_clarification/")
library(tidyverse)
library(stringr)
#read the raw table
d0<-read.table(file = 'MELR.txt', sep = '\t', header = TRUE)

#add Sample.Sample_ID column
d1<- d0 %>%
    #add column "Sample.Sample_ID"
     add_column("Sample.Sample_ID" ="", .before="MELR.BARCODE..STICKER...NUMBER.") %>%
     rename("Sample.External_ID" = "MELR.BARCODE..STICKER...NUMBER.") %>%
     unite(col = Participant.Second_Reference, c("PATIENT.STUDY.ID", "PATIENT.INITIALS"), sep="_", remove=TRUE) %>%
     add_column("Sample.Sample_Type"="Frozen", .before="SAMPLE.TYPE......TUBE.NUMBER") %>%
    #Add a column "Sample.Tissue_Type" and the column value based on "SAMPLE.TYPE......TUBE.NUMBER"
     mutate( Sample.Tissue_Type = case_when(
       SAMPLE.TYPE......TUBE.NUMBER == "PLASMA" ~ "Plasma",
       SAMPLE.TYPE......TUBE.NUMBER == "BUFFY COAT" ~ "Buffy Coat",
       SAMPLE.TYPE......TUBE.NUMBER == "FFPE block" ~ "FFPE block",
       SAMPLE.TYPE......TUBE.NUMBER == "WHOLE BLOOD" ~ "Whole Blood")) %>%
    #Add a column "Sample.Tissue_sub-type", and the column value based on "SAMPLE.TYPE......TUBE.NUMBER"
     mutate( "Sample.Tissue_sub-type"=case_when(
       SAMPLE.TYPE......TUBE.NUMBER == "PLASMA" ~ "Plasma EDTA",
       SAMPLE.TYPE......TUBE.NUMBER == "BUFFY COAT" ~ "Buffy Coat",
       SAMPLE.TYPE......TUBE.NUMBER == "FFPE block" ~ "FFPE block",
       SAMPLE.TYPE......TUBE.NUMBER == "WHOLE BLOOD" ~ "Whole Blood")) %>%
    add_column("Sample.Container_Type"="2mL screw top microcentrifuge tube") %>%
    transform(ALIQUOT.VOLUME..ML.=as.character(ALIQUOT.VOLUME..ML.))%>%
    #Add a column "Sample.Volume", and the column value based on "ALIQUOT.VOLUME..ML."
    mutate( "Sample.Volume"=case_when(
        #the code below equals to: if (ALIQUOT.VOLUME..ML. == "N/A"){Sample.Volume="N/A}
        ALIQUOT.VOLUME..ML. == "N/A" ~ "N/A",
        #TRUE means "all the remaining rows"
       TRUE ~ ALIQUOT.VOLUME..ML.
       )) %>%
    #Add a column "Sample.Volume_Unit", and the column value based on "Sample.Voolume"
    mutate( "Sample.Volume_Unit"=case_when(
       Sample.Volume == "N/A" ~ "N/A",
       TRUE ~ "ml"
       )) %>%
    #Change "BLOOD.COLLECTION.DEVICE" string value from UPPER CASE to Title Case
    mutate("Collection_Event.Collection_Method"=str_to_title(BLOOD.COLLECTION.DEVICE)) %>%
    #Add "Collection_Event.Event_Date" column
    mutate(Collection_Event.Event_Date=case_when(
        is.na(COLLECTION.DATE) ~ "00/00/0000",
        COLLECTION.DATE=="N/A" ~ "00/00/0000",
        TRUE ~ as.character(COLLECTION.DATE)
    )) %>%
    #Add "Collection_Event.Event_Time" column
    mutate(Collection_Event.Event_Time=case_when(
        is.na(COLLECTION.TIME) ~ "00:00",
        COLLECTION.TIME=="N/A" ~ "00:00",
        TRUE ~ substr(strptime(COLLECTION.TIME,format = "%H.%M"),12,16)
    )) %>%
    unite(col = Sample.DateTime_Taken, c("Collection_Event.Event_Date", "Collection_Event.Event_Time"), sep=" ", remove=F) %>%
    mutate(Sample.Processed_By = PROCESSED.BY  ) %>%
    #add a tmp column containing the data with right time format
    mutate(CENTRIFUGATION.TIME_tmp=case_when(
        is.na(CENTRIFUGATION.TIME) ~ "00:00",
        CENTRIFUGATION.TIME == "N/A" ~ "00:00",
       TRUE ~ substr(strptime(CENTRIFUGATION.TIME,format = "%H.%M"),12,16)
    )) %>%
    #paste0 means concatenate characters seamlessly
    mutate(Sample.Processed_Date_Time=paste0(as.character("00/00/0000")," ",CENTRIFUGATION.TIME_tmp)) %>%
    mutate("Sample.Procedure_Used "= case_when(
       Sample.Processed_By !="N/A" ~ "Centrifugation",
       TRUE ~ "N/A"
    )) %>%
    mutate(Sample.Frozen_Date=paste0(as.character("00/00/0000")," ",substr(strptime(FREEZING.TIME,format = "%H.%M"),12,16))) %>%
    mutate("Sample.Passage"=ifelse(Sample.Volume ==1,0,NA )) %>%
    add_column("Storage_Location.Storage_Reference"="") %>%
    rename(Storage_Sub_Location.Reference= MELR.BOX.NUMBER...............EG..CAM.001............OX.001.) %>%
    #Add a column "Sample.Storage_Grid_Reference", use two digits in the location instead of one digit, e.g. changing "A1" to "A01"
    mutate(Sample.Storage_Grid_Reference= gsub('^([A-Z]+)([0-9]+)$', "\\10\\2",ROW...COLUMN )) %>%
    add_column("Sample.PROJECTNO"="","SAMPLE.TEAM"="CRUK-CI") %>%
    #remove these columns
    select(-one_of("ROW...COLUMN","FREEZING.TIME","CENTRIFUGATION.TIME","COMMENTS.ON.DATA.TRANSFER.TO.CRI","I","DATE.TRANSFERRED.TO.CRI")) %>%
    select(-one_of("COLLECTION.DATE","COLLECTION.TIME","PROCESSED.BY","SAMPLE.TYPE......TUBE.NUMBER","ALIQUOT.VOLUME..ML.","BLOOD.COLLECTION.DEVICE","CENTRIFUGATION.TIME_tmp"))

write.csv(d1,file = "clean_dataframe.csv")













