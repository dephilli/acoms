connect<-function(){
  connections<-odbc::odbcListDataSources()

  connections$test<-grepl('INFORMIX', connections$description)

  connect<-connections[connections$test==TRUE,]

  connect<-connect[1,]

  odbcname<-connect$name

  a<-DBI::dbConnect(odbc::odbc(), odbcname, timeout = 10)
  return(a)}
