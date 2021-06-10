library(hash)

test_file <- read.xlsx("/Users/wang04/Documents/phd_projects/hackthon/EXP1817_JW_layout.xlsx" ,1)
todo_file <- xlsx::read.xlsx("~/Documents/phd_projects/hackthon/slurp_mutation.xlsx", 1)
test_file_list <- list()

df_to_list <- function(file) {
    file_col_list  <- list()
    for (i in 1:ncol(file)) {
    file_col_list[[i]] <- file[, i]
    }
    names(file_col_list) <- colnames(file)
    
    return(file_col_list)
}



test_file_list <- df_to_list(test_file)

f2 <- function(x, 
               col_name, 
               todo_file ){
    
    todo_file <- xlsx::read.xlsx(todo_file, sheetIndex = 1)
    
    if(col_name %in% todo_file$col) {
        
        message(paste("handling", col_name, "..."))
        todo_file2 <- dplyr::filter(todo_file, col == !!col_name)
        dictionary <- hash(keys = todo_file2$before, values = todo_file2$after)
        
        lapply(x, function(i){if(i %in% keys(dictionary)) message(paste0("-----> found ",i))})
        
        result <- unlist(lapply(x, function(i){if_else(i %in% keys(dictionary), values(dictionary)[i], i)}) )
        
       return(result)
        
    } else {return(x)}
}


file_df <- mapply(f2, 
       x = test_file_list, 
       col_name = names(test_file_list), MoreArgs = list(todo_file =  "/Users/wang04/Documents/phd_projects/hackthon/slurp_mutation.xlsx") )

write.xlsx(file_df, file = , row.names = FALSE, showNA = FALSE)

##############################################



a <- "sequencer"
test_file_list[a]
> x <- list(a=11, b=12, c=13)
> Map(function(x, i, todo_file) paste(i, x), x, names(x))