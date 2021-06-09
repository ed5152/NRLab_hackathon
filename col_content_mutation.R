outputpath <- "/Volumes/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4"



dir.create("/Volumes/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4", 
            recursive = TRUE)

xy.df <- xlsx::read.xlsx("~/Documents/phd_projects/hackthon/todo.xlsx", 1)


colnames(xy.df) <- c("file")
xy.df < xy.df %>%
    dplyr::mutate(file = file.path(outputpath, paste0(file, "_layout.xlsx")))

xy.list <- split(xy.df, seq(nrow(xy.df)))

for (file in xy.df) file.copy(file, "/Volumes/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4")

##############################################################################################


outputpath <- "/Volumes/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4"


target_fullname<- 
    list.files(path = outputpath, 
               full.names = TRUE)
target_fullname_list <- as.list(target_fullname)
names(target_fullname_list)  <- target_fullname


##############################################################################################

f <- function(x) {
    library(xlsx)
    library(tidyverse)
    file <- xlsx::read.xlsx(file = x, 1)
    file<- as.data.frame(file)
    
    
    if("sequencer" %in% colnames(file)){
        file <- file %>% dplyr::mutate( 
            sequencer = case_when(
                .data$sequencer == "HiSeq_4500" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4501" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4502" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4503" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4504" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4505" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4506" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4507" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4508" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4509" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4510" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4511" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4512" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4513" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4514" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4515" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4516" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4517" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4518" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_4519" ~"HiSeq_4000",
                .data$sequencer == "HiSeq_2501" ~"HiSeq_2500",
                .data$sequencer == "HiSeq_2502" ~"HiSeq_2500",
                .data$sequencer == "HiSeq_2503" ~"HiSeq_2500",
                .data$sequencer == "HiSeq_2504" ~"HiSeq_2500",
                .data$sequencer == "HiSeq_2505" ~"HiSeq_2500",
                TRUE ~ as.character(.data$sequencer)))
        
        
        
        
        
    }
    
    
    if("library_prep" %in% colnames(file)){
        file <- file %>% dplyr::mutate( 
            library_prep = case_when(
                .data$library_prep == "Agilent_SureSelectXT" ~ "Agilent_XT",
                .data$library_prep == "SureSelectXT" ~ "Agilent_XT",
                .data$library_prep == "Agilent XTHS" ~ "Agilent_XTHS",
                .data$library_prep == "XTHS" ~ "Agilent_XTHS",
                .data$library_prep == "Agilent_SureSelectXTHS" ~ "Agilent_XTHS",
                .data$library_prep == "SMART" ~ "DNA_SMART_ChIP_seq",
                .data$library_prep == "Truplex_DNA_seq" ~ "Thruplex_DNA_seq",
                .data$library_prep == "Thruplex_DNA_Seq" ~ "Thruplex_DNA_seq",
                .data$library_prep == "Truplex_DNA_Seq" ~ "Thruplex_DNA_seq",
                .data$library_prep == "Thruplex_dna_seq" ~ "Thruplex_DNA_seq",
                .data$library_prep == "Rubicon_DNA_seq" ~ "Thruplex_DNA_seq",
                .data$library_prep == "Thruplex_Plasma_Seq" ~ "Thruplex_plasma_seq",
                .data$library_prep == "Thruplex_Plasma_seq" ~ "Thruplex_plasma_seq",
                .data$library_prep == "Plasma_seq" ~ "Thruplex_plasma_seq",
                .data$library_prep == "Plasma-Seq" ~ "Thruplex_plasma_seq",
                .data$library_prep == "PlasmaSeq" ~ "Thruplex_plasma_seq",
                .data$library_prep == "Thruplex_taq_seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Thruplex_Tag_Seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Thruplex_tag_seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Thruplex_TAG_seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Thruplex_Taq_seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Rubicon_Tag_Seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Tag_seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Tag-seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Taq-Seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Tag-Seq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "TagSeq" ~ "Thruplex_Tag_seq",
                .data$library_prep == "Thruplex_TagSeq" ~ "Thruplex_Tag_seq",
                
                TRUE ~ as.character(.data$library_prep)))
    }
    
    if("capture_protocol" %in% colnames(file)){
        file <- file %>% dplyr::mutate( 
            capture_protocol = case_when(.data$capture_protocol =="Illumina_TruSeq_Rapid_Exome" ~ "Illumina_TruSeq_exome",
                                         .data$capture_protocol =="Illumina_Truseq_Rapid_Exome" ~ "Illumina_TruSeq_exome",
                                         .data$capture_protocol =="SureSelect_Target_Enrichment_System_for_Illumina_Paired_end" ~ "Agilent_XT",
                                         .data$capture_protocol =="Illumina_TruSeq_Exome" ~ "Illumina_TruSeq_exome",
                                         .data$capture_protocol =="Illumina_Truseq_Exome" ~ "Illumina_TruSeq_exome",
                                         .data$capture_protocol =="Agilent_SureSelectXT" ~ "Agilent_XT",
                                         .data$capture_protocol =="Agilent_sureSelect_XT_custom_capture" ~ "Agilent_XT",
                                         .data$capture_protocol =="Agilent_SureSelectXT_Custom" ~ "Agilent_XT",
                                         .data$capture_protocol =="Agilent_SureSelectXT_Custom" ~ "Agilent_XT",
                                         .data$capture_protocol =="na" ~ "Not_captured",
                                         .data$capture_protocol =="Illumina_TruSeq" ~ "Illumina_TruSeq_exome",
                                         .data$capture_protocol =="Nextera_Rapid_Capture" ~ "Illumina_TruSeq_exome",
                                         .data$capture_protocol =="XTHS_custom" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="Agilent_XTHS_B0" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="XTHS" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="Agilent XTHS" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="Agilent_SureSelectXTHS" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="Agilent_XTHS_Custom_Panel_renal" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="Agilent_XTHS_C0" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="Agilent XTHS LUCID batch2" ~ "Agilent_XTHS",
                                         .data$capture_protocol =="" ~ "Not known (historic)",
                                         .data$capture_protocol ==" " ~ "Not known (historic)",
                                         TRUE ~ as.character(.data$capture_protocol)))
    }
    
    
    
    if("quantification_method" %in% colnames(file)){
        file <- file %>% dplyr::mutate( 
            quantification_method = case_when(
                .data$quantification_method == "PheraStar" ~ "Qubit",
                .data$quantification_method == "Qbit" ~ "Qubit",
                .data$quantification_method == "qdPCR_37_K" ~ "Qubit",
                .data$quantification_method == "qubit" ~ "Qubit",
                .data$quantification_method == "" ~ "Not known (historic)",
                .data$quantification_method == " " ~ "Not known (historic)",
                TRUE ~ as.character(.data$quantification_method)))}
    
    if("data_type" %in% colnames(file)){
        file <- file %>% dplyr::mutate( 
            data_type = case_when(
                .data$data_type == "Tag-seq" ~ "TagSeq",
                TRUE ~ as.character(.data$data_type)))}
    
    
    xlsx::write.xlsx(x = file, file = x, row.names = FALSE, showNA = FALSE)
    message(paste("done", x))
    return("success")
}





##############################################################################################
strange_file <- "/Volumes/files/research/nrlab/group/group_folders/GROUP/Hackathon/LAYOUT/copy_LAYOUTS/haichao4/EXP1846_KH_layout.xlsx"

target_fullname_list2 <- target_fullname_list[target_fullname_list != strange_file]

lapply(target_fullname_list2, f)


##############################################################################################


