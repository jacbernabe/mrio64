library(DBI)
library(duckdb)
library(dplyr)
library(glue)
library(openxlsx)
library(cli)

#load concordance tables
load("data/hscodes.rda")
load("data/hsbecsitc.rda")
load("data/hsbec.rda")
load("data/hs12cpc21.rda")
load("data/cpc21isic4.rda")
load("data/isic4mrio64.rda")

# test the mapping logic in one file
file <- "/Users/johnarvinbernabe/Documents/MRIO - Canada/CAN 2018.gz"
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir=":memory:")

df_cleaned <- DBI::dbGetQuery(conn, glue("
  SELECT period,
         reporterCode,
         flowCode,
         partnerCode,
         classificationCode,
         cmdCode,
         primaryValue
  FROM read_csv_auto('{file}', all_varchar=1)
  WHERE partnerCode <> 0
    AND partner2Code = 0
    AND (flowCode = 'X' OR flowCode = 'M')
    AND length(cmdCode) = 6
    AND motCode = 0
    AND customsCode = 'C00'
"))

# To match concordances (integers)
df_cleaned$cmdCode <- as.numeric(df_cleaned$cmdCode)

# Determine in the HS edition

hscode <- hscodes$edition[hscodes$code == unique(df_cleaned$classificationCode)]
hscode

# Map the df_cleaned
df_sectorized <- suppressMessages(
  dplyr::mutate(
    dplyr::left_join(
      dplyr::left_join(
        dplyr::left_join(
          dplyr::left_join(
            dplyr::left_join(
              df_cleaned,
              dplyr::select(hsbecsitc,
                            { { hscode } },
                            HS12,
                            BEC5
              ),
              by = stats::setNames(hscode, "cmdCode"),
              multiple = "any"
            ),
            dplyr::select(hsbec,
                          BEC5 = BEC5Code1,
                          BEC5EndUse
            ),
            multiple = "any"
          ),
          hs12cpc21,
          multiple = "any"
        ),
        cpc21isic4,
        multiple = "any"
      ),
      isic4mrio64,
      multiple = "any"
    ),
    period       = as.numeric(period),
    reporterCode = as.numeric(reporterCode),
    partnerCode  = as.numeric(partnerCode),
    primaryValue = as.numeric(primaryValue)
  )
)

# Aggregate to MRIO sectors and end-use categories
df_aggregated <- df_sectorized %>%
  dplyr::group_by(
    period,
    reporterCode,
    flowCode,
    partnerCode,
    BEC5EndUse,
    MRIO
  ) %>%
  dplyr::summarise(
    primaryValue = sum(primaryValue, na.rm = TRUE),
    .groups = "drop"
  )

# Export to CSV
write.csv(df_aggregated, "/Users/johnarvinbernabe/Documents/MRIO - Canada/CAN_2018_aggregated.csv",
          row.names = FALSE)

# Or to Excel
wb <- openxlsx::createWorkbook()
openxlsx::addWorksheet(wb, "Sheet1")
openxlsx::writeData(wb, "Sheet1", df_aggregated)
openxlsx::saveWorkbook(wb, "/Users/johnarvinbernabe/Documents/MRIO - Canada/CAN_2018_aggregated.xlsx", overwrite = TRUE)



