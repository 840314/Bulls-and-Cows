library(tidyverse)
library(magrittr)
library(tibble)
library(gtools)
library(shiny)
library(ggplot2)

ui <- fluidPage(
  titlePanel("Simulations"),
  sidebarLayout(
    sidebarPanel(
      selectInput("game", "Choose a game:", 
                  c("Bulls and Cows" = "BC", "AA" = "LL")),
      conditionalPanel(
        condition = "input.game == 'BC'",
        sliderInput("obs", "Number of simulations:", 
                    min = 0, max = 1000, value = 10),
        checkboxGroupInput("algrithm", "Choose an algrithm:",
                           c("algrithm1" = "alg1", "algrithm2" = "al2"))
      ), width = 3
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Table", tableOutput("table")),
        tabPanel("Plot", plotOutput("plot"))
      )
    )
  )
)

server <- function(input, output) {
  data <- reactive({
    table <- permutations(10,4,as.character(0:9)) %>% as.tibble
    total <- data.frame()
    for(i in 1:5040){
      total[i,1] <- paste(table[i,], collapse = "") 
    }; rm(i,table)
    
    simulations <- input$obs
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
    
    data <- data.frame(n = 1:simulations, times = times) %>%
      mutate(times = cumsum(times)/n)
    return(data)
  })
  output$table <- renderTable({
    data <- data()
    return(data)
  })
  
  output$plot <- renderPlot({
    data <- data()
    p <- ggplot(data, aes(x = n, y = times)) + 
      geom_line() +
      labs(x = "simulations", y = "average guess times", title = "")
    return(p)
  })
}


shinyApp(ui = ui, server = server)
