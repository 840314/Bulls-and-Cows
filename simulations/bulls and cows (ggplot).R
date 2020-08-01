library(tidyverse)
library(magrittr)
library(tibble)
library(gtools)
table <- permutations(10,4,as.character(0:9)) %>% as.tibble
total <- data.frame()
for(i in 1:5040){
  total[i,1] <- paste(table[i,], collapse = "") 
}; rm(i,table)

simulations <- 2000
times <- c()

start_time <- Sys.time()
for(i in 1:simulations){
  check <- function(guess, answer){
    guess %<>% strsplit(split = "") %>% unlist
    answer %<>% strsplit(split = "") %>% unlist
    
    A <- sum(guess == answer)
    B <- sum(guess%in%answer)-A
    return(paste0(A,"A",B,"B"))
  }
  
  population <- total
  answer <- population[sample(1:nrow(population),1),]
  n <- 0
  
  while(nrow(population)!=1){
    guess <- population[sample(1:nrow(population),1),]
    n <- n + 1
    result <- check(guess, answer)
    
    population %<>% apply(FUN = check, 1, answer = guess) %>% cbind(population, tmp = .) %>% 
      filter(tmp==result) %>% select(V1)
  }
  times <- c(times, n)
}
Sys.time() - start_time
average <- mean(times)

library(ggplot2)
data <- data.frame(n = 1:simulations, times = times) %>%
  mutate(times = cumsum(times)/n)
ggplot(data, aes(x = n, y = times)) + 
  geom_line() +
  labs(x = "simulations", y = "average guess times", title = "Results") 







