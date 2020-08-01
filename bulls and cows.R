####  Backward check  ####
library(tidyverse)
library(magrittr)
library(tibble)
library(gtools)
table <- permutations(10,4,as.character(0:9)) %>% as.tibble
population <- data.frame()
for(i in 1:5040){
  population[i,1] <- paste(table[i,], collapse = "") 
}; rm(i,table)

check <- function(guess, answer){
  guess %<>% strsplit(split = "") %>% unlist
  answer %<>% strsplit(split = "") %>% unlist
  
  A <- sum(guess == answer)
  B <- sum(guess%in%answer)-A
  return(paste0(A,"A",B,"B"))
}

answer <- population[sample(1:nrow(population),1),]
n <- 0

while(nrow(population)!=1){
  guess <- population[sample(1:nrow(population),1),]
  n <- n + 1
  result <- check(guess, answer)
  
  population %<>% apply(FUN = check, 1, answer = guess) %>% cbind(population, tmp = .) %>% 
    filter(tmp==result) %>% select(V1)
}