lapply(c('VennDiagram', 'viridis', 'venneuler', 'eulerr'),
       function(pkg_name) { if(! pkg_name %in% installed.packages()) { install.packages(pkg_name)} } )

library(VennDiagram)
library(dplyr)
library(viridis)

# venn diagram 1 -  diagnosis types

# Create a Venn diagram with appropriate labels
venn.plot <- venn.diagram(
  x = list(
    "Pulmonary heart disease" = which(dataset$PHD),
    "All cause PH" = which(dataset$AC1),
    "Clean PAH definition 1" = which(dataset$Clean_PAH1),
    "Clean PAH definition 2" = which(dataset$Clean_PAH2)
  ),
  filename = NULL, # Do not save to a file, display plot instead
  output = TRUE,
  imagetype = "png",  # You can use other formats like pdf if you wish
  height = 480,
  width = 480,
  resolution = 300,
  col = viridis(4),
  fill =  viridis(4),
  alpha = 0.5,
  cat.col =  viridis(4),
  cat.cex = 1.2,
  cex = 1.2,
  main = "Venn Diagram of Overlap Between ICD 10 diagnoses",
  main.cex=2
)

# Display the Venn diagram
grid.newpage()
grid.draw(venn.plot)


# prettier option
library(eulerr)

# Convert lists to sets for easier operations
A <- dataset$PHD==1
B <- dataset$AC1==1
C <- dataset$Clean_PAH1==1
D <- dataset$Clean_PAH2==1


set_list <- cbind(A = A, B = B, C = C, D = D)

fit2 <- euler(set_list)

plot(
  fit2,
  quantities = TRUE,
  fill = viridis(4, alpha = 0.5),
  edges= viridis(4),
  lty = 1:3,
  labels = c("Pulmonary heart disease", "All cause Pulmonary Hypertension", "Pulmonary Arterial Hypertension definition 1", 
    "Pulmonary Arterial Hypertension definition 2"),
)
