# Script to merge MI Export CSV Files
# JD Piquard, 2016 November
# (c) Portr Ltd

require(lubridate)

###DEFINE SOME VARIABLES
# Set the VAT rate!!!
vatRate <- 1.2
#Define your csv export settings
folderPath <- '~/Google Drive/PortrHacks/dataCrunching/Merging Folder/mergedResults/'
File_name <- "mergedBookings"

#startBookingDate <- as.Date('10/10/2016', format = "%d/%m/%Y")
#endBookingDate <- as.Date('05/11/2017', format = "%d/%m/%Y")

###LOAD THE DATA SOURCES###
datapath <- '~/Google Drive/PortrHacks/dataCrunching/Merging Folder/source data/report.csv'
datapath2 <- '~/Google Drive/PortrHacks/dataCrunching/Merging Folder/source data/landing_pages.csv'
datapathSDS <- '~/Google Drive/PortrHacks/dataCrunching/Merging Folder/source data/bookings.csv'

#Load Data: ABC bookings
bookingsABC <- read.csv(datapath)

#Load Data: SDS bookings
bookingsSDS2 <- read.csv(datapathSDS)

###CLEAN UP DATA SOURCES###

#remove any duplicates
bookingsABC <- bookingsABC[!duplicated(bookingsABC[,1]), ]
#Remove cancelled bookings
#bookingsABC <- bookingsABC[bookingsABC$Cancelled==F,]

#clean up some names
colnames(bookingsABC)[1] <- 'Booking_reference'
colnames(bookingsSDS2)[1] <- 'Booking_reference'
colnames(bookingsABC)[51] <- 'Hand_luggage_No'

#clean up some values for consistency
#paymentValues
bookingsABC$Booking_value_gross_total <- bookingsABC$transaction_payment_total/100
bookingsABC$transaction_payment_total <- bookingsABC$Card_Payment_Amount/100
bookingsABC$Card_Payment_Amount <- NULL
#zones in ABC
bookingsABC$Zone <- substr(bookingsABC$Zone, 10, 11)
#direction in ABC
bookingsABC$Journey_direction  <- 'GeneralLocationToAirport'
#booking type in ABC
bookingsABC$Department <- 'prebook'
#dates
bookingsABC$Booking_date  <- as.Date(bookingsABC$Booking_date, format = "%d/%m/%Y")
bookingsSDS2$Booking_date  <- as.Date(bookingsSDS2$Booking_date, format = "%d/%m/%Y")
#cancelled boolean
bookingsSDS2$Cancelled <- as.logical(bookingsSDS2$Cancelled)

###MERGE###

#identify columns which are not present in each spreadsheet and even them out
columnsToDeleteSDS <- names(bookingsSDS2)[!(names(bookingsSDS2) %in% names(bookingsABC))]
columnsToDeleteABC <- names(bookingsABC)[!(names(bookingsABC) %in% names(bookingsSDS2))]

bookingsSDS2[columnsToDeleteABC] <- NA
bookingsABC[columnsToDeleteSDS] <- NA

#actually merge the csv files
bookings.merged <- rbind(bookingsABC,bookingsSDS2)
bookings.merged <- bookings.merged[with(bookings.merged, order(Booking_date)), ]

###CLEANUP MERGED DATASET###
# Calculate transaction payment without VAT
bookings.merged$transaction_payment_total_ex_VAT <- bookings.merged$transaction_payment_total/vatRate
# Standardise airport names into new column
bookings.merged$airportCode <- bookings.merged$Airport
bookings.merged$airportCode <- gsub('.*Heathrow.*','LHR',bookings.merged$airportCode,ignore.case = T)
bookings.merged$airportCode <- gsub('.*Gatwick.*','LGW',bookings.merged$airportCode,ignore.case = T)
bookings.merged$airportCode <- gsub('.*City.*','LCY',bookings.merged$airportCode,ignore.case = T)
# Correct some dates
bookings.merged$Outward_Journey_Luggage_Collection_date  <- as.Date(bookings.merged$Outward_Journey_Luggage_Collection_date, format = "%d/%m/%Y")
bookings.merged$Outward_Journey_Luggage_drop_off_date  <- as.Date(bookings.merged$Outward_Journey_Luggage_drop_off_date, format = "%d/%m/%Y")

# Find out the airline code being used
bookings.merged$airlineCode <- toupper(ifelse(bookings.merged$Out.bound_flt_code != "", 
                                      substr(bookings.merged$Out.bound_flt_code, 1, 2),
                                      substr(bookings.merged$In.bound_flt_code, 1, 2)))
# Clean up standard product name
bookings.merged$Product_name <-gsub('Standard airportr delivery service','SDS',bookings.merged$Product_name)

# Create an isBA column
bookings.merged$isBA <- grepl('BA',bookings.merged$Out.bound_flt_code,ignore.case = T)|grepl('BA',bookings.merged$In.bound_flt_code,ignore.case = T)

# Create an combined product column to simplify sales reporting
bookings.merged$combinedProductCategory <- paste(bookings.merged$Product_name, bookings.merged$Journey_direction, bookings.merged$isBA)

# Create columns for Week, Year and Month Numbers
bookings.merged$Booking_Year <- year(bookings.merged$Booking_date)
bookings.merged$Booking_Month <- month(bookings.merged$Booking_date)
bookings.merged$Booking_Week <- isoweek(bookings.merged$Booking_date)

bookings.merged$Collection_Year <- year(bookings.merged$Outward_Journey_Luggage_Collection_date)
bookings.merged$Collection_Month <- month(bookings.merged$Outward_Journey_Luggage_Collection_date)
bookings.merged$Collection_Week <- isoweek(bookings.merged$Outward_Journey_Luggage_Collection_date)

bookings.merged$drop_off_Year <- year(bookings.merged$Outward_Journey_Luggage_drop_off_date)
bookings.merged$drop_off_Month <- month(bookings.merged$Outward_Journey_Luggage_drop_off_date)
bookings.merged$drop_off_Week <- isoweek(bookings.merged$Outward_Journey_Luggage_drop_off_date)


##########################################################################################################
### OUTPUT ###

### Save to CSV
# All Bookings including cancelled
write.csv(bookings.merged, paste(folderPath,File_name,"WithCancelled.csv",sep=""))
# Only Non-Cancelled Bookings
#Remove cancelled bookings
notCancelledBookings.merged <- bookings.merged[bookings.merged$Cancelled==F,]
write.csv(notCancelledBookings.merged, paste(folderPath,File_name,".csv",sep=""))

### Create BA Report
reportBA <- notCancelledBookings.merged[,c("Flt_booking._ref",
                                           "Booking_reference",
                                           "Booking_date",
                                           "Outward_Journey_Luggage_drop_off_date",
                                           "Product_name",
                                           "Airport",
                                           "Total_luggage_No",
                                           "airlineCode",
                                           "Out.bound_flt_code",
                                           "Out.bound_dest_apt_code",
                                           "Out.bound_dest_city_name",
                                           "Out.bound_flt_time",
                                           "transaction_payment_total_ex_VAT",
                                           "Promocodes")]
reportBA <- reportBA[notCancelledBookings.merged$airlineCode=='BA',]
write.csv(reportBA, paste(folderPath,'BAReport',File_name,".csv",sep=""))

### Generating Daily Ops Reports

# Creating folder structure
todaysDate <- Sys.Date()
todaysOpsFolderPath <- paste(folderPath, 'opsData/',todaysDate,"/",sep="")
dir.create(todaysOpsFolderPath)
yesterdaysDate <- Sys.Date() - 1
yesterdaysOpsFolderPath <- paste(folderPath, 'opsData/',yesterdaysDate,"/",sep="")
dir.create(yesterdaysOpsFolderPath)

# Daily Collections Report
dailyCollections <- notCancelledBookings.merged[notCancelledBookings.merged$Outward_Journey_Luggage_Collection_date==todaysDate,]
#dailyCollections <- dailyCollections[with(dailyCollections, order(Outward_Journey_Luggage_Collection_time)), ]
write.csv(dailyCollections, paste(todaysOpsFolderPath,'opsCollections',todaysDate,".csv",sep=""))

# Daily Injections and Repatriations Report
dailyInjectionsRepatriations <- notCancelledBookings.merged[notCancelledBookings.merged$Outward_Journey_Luggage_drop_off_date==todaysDate,]
write.csv(dailyInjectionsRepatriations, paste(todaysOpsFolderPath,'opsInjectionsAndRepatriations',todaysDate,".csv",sep=""))

# Update Yesterdays ops reports with latest data
# Collections
yesterdaysCollections <- notCancelledBookings.merged[notCancelledBookings.merged$Outward_Journey_Luggage_Collection_date==yesterdaysDate,]
write.csv(yesterdaysCollections, paste(yesterdaysOpsFolderPath,'opsCollections',yesterdaysDate,".csv",sep=""))
# Injections/Repatriations
yesterdaysInjectionsRepatriations <- notCancelledBookings.merged[notCancelledBookings.merged$Outward_Journey_Luggage_drop_off_date==yesterdaysDate,]
write.csv(yesterdaysInjectionsRepatriations, paste(yesterdaysOpsFolderPath,'opsInjectionsAndRepatriations',yesterdaysDate,".csv",sep=""))

