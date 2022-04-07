connect<-function(){
  dsn <- svDialogs::dlgInput("Acoms odbc name", "acoms")$res

  con<-DBI::dbConnect(odbc::odbc(),dsn, timeout=10)
  return(con)
}


#inst reports

institutional_report <- function(dt, excel = TRUE) {
  if (missing(dt)) {
    dt <- lubridate::floor_date(Sys.Date(), unit = "month") - 1
  }
  else{
    dt <- as.Date(dt)
  }


  md <- months(dt)
  yt <- lubridate::year(dt)
  dy <- lubridate::day(dt)
  date_field <- paste0(md, " ", yt)
  date_field_p <- paste0(md, " ", dy, ", ", yt)
  date_run <- Sys.Date()
  month_run <- months(date_run)
  day_run <- lubridate::day(date_run)
  year_run <- lubridate::year(date_run)
  date_run <- paste0(month_run, " ", day_run, ", ", year_run)
  sql_mo <- lubridate::month(dt)
  sql_date <- paste0(sql_mo, "/", dy, "/", yt)
  con <- connect()
  p_query <-
    glue::glue_sql(
      "select distinct a.ofndr_num, body_loc_desc, race_cd, sex, lgl_stat_cd, dob, assgn_dt
               from (select *, case when end_dt is null then today else end_dt end as end_date from ofndr_loc_hist) a
               join body_loc_cd b
               on a.body_loc_cd=b.body_loc_cd
               join ofndr c
               on a.ofndr_num=c.ofndr_num
               left join (select * from (select ofndr_num, lgl_stat_cd, stat_beg_dt, case when stat_end_dt is null then today else stat_end_dt end as stat_end from ofndr_lgl_stat) where stat_beg_dt<=Date('{`sql_date`}') and stat_end>Date('{`sql_date`}'))d on a.ofndr_num=d.ofndr_num
                join (select distinct ofndr_num, dob
from
(select distinct ofndr_num, ranker, name_id, dob, row_number() over(partition by ofndr_num order by ranker, name_id ) as row1
from
(select distinct ofndr_num, b.name_id, dob, case when dob_typ_cd='D' then 0 else 1 end as ranker
from ofndr_dob a join ofndr_name b
on a.name_id=b.name_id
order by ofndr_num, 4, b.name_id))
where row1=1)e
on a.ofndr_num=e.ofndr_num
               where assgn_dt<=Date('{`sql_date`}') and end_date>Date('{`sql_date`}')
               and loc_typ_cd='C'",
      .con = con
    )



  prison <- DBI::dbGetQuery(con, p_query)


  prison <- prison %>%
    dplyr::mutate(body_loc_desc = stringr::str_trim(body_loc_desc)) %>%
    dplyr::mutate(
      body_loc_desc = dplyr::case_when(
        body_loc_desc == 'ANCHORAGE JAIL' ~ 'Anchorage Jail',
        body_loc_desc == 'COOK INLET PRETRIAL' ~
          'Anchorage Jail',
        body_loc_desc == "WILDWOOD CC" ~
          'Wildwood CC',
        body_loc_desc == 'WILDWOOD TRANSITIONAL' ~ 'Wildwood CC',
        body_loc_desc == 'WILDWOOD PRETRIAL' ~ 'Wildwood Pretrial',
        stringr::str_starts(body_loc_desc, 'PALMER') ~ 'Palmer CC',
        body_loc_desc == 'ANVIL MTN CC' ~ 'Anvil Mtn',
        body_loc_desc == 'FAIRBANKS CC' ~ 'Fairbanks CC',
        body_loc_desc == 'GOOSE CREEK CC' ~ 'Goose Creek CC',
        body_loc_desc == 'HILAND MTN CC' ~ 'Hiland Mtn CC',
        body_loc_desc == 'KETCHIKAN CC' ~ 'Ketchikan CC',
        body_loc_desc == 'LEMON CREEK CC' ~ 'Lemon Creek CC',
        body_loc_desc == 'MATSU PRETRIAL' ~ 'Mat-Su Pretrial',
        body_loc_desc == 'PT. MACKENZIE CF' ~ 'Pt. Mackenzie',
        body_loc_desc == 'SPRING CREEK CC' ~ 'Spring Creek CC',
        body_loc_desc == 'YUKON-KUSKOKWIM CC' ~ 'Yukon-Kuskokwim CC',
        TRUE ~ stringr::str_to_title(body_loc_desc)
      )
    ) %>%
    dplyr::mutate(sex = dplyr::case_when(sex == 'F' ~ 'Female',
                                  sex == 'M' ~ 'Male',
                                  TRUE ~ 'Transgender')) %>%
    dplyr::mutate(
      lgl_stat_cd = dplyr::case_when(
        lgl_stat_cd == "C" ~ 'Sentenced',
        lgl_stat_cd == '1' ~ 'Non-Criminal',
        lgl_stat_cd == 'X' ~ 'Federal Inmate',
        TRUE ~ 'Unsentenced'
      )
    ) %>%
    dplyr::mutate(age = trunc((dob %--% dt) / lubridate::years(1))) %>%
    dplyr::mutate(
      age_group = dplyr::case_when (
        age < 20 ~ '16-19',
        age < 25 & age >= 20 ~ '20-24',
        age < 30 & age >= 25 ~ '25-29',
        age < 35 & age >= 30 ~ '30-34',
        age < 40 & age >= 25 ~ '35-39',
        age < 45 & age >= 40 ~ '40-44',
        age < 50 & age >= 45 ~ '45-49',
        age < 55 & age >= 50 ~ '50-54',
        age < 60 & age >= 55 ~ '55-59',
        age < 65 & age >= 60 ~ '60-64',
        age < 70 & age >= 65 ~ '65-69',
        age < 75 & age >= 70 ~ '70-74',
        age < 80 & age >= 75 ~ '75-79',
        age >= 80 ~ '80+',
        TRUE ~ 'error'
      )
    ) %>%
    dplyr::mutate(
      race_type = dplyr::case_when(
        race_cd %in% c("K", "L", "I", "N", "C", "D", "E", "F", "G", "J", "O", "Q") ~
          'Alaska Native',
        race_cd %in% c('A', 'P', 'R') ~ "Asian Pacific Islander",
        race_cd == 'B' ~ 'Black',
        race_cd == 'H' ~ 'Hispanic',
        race_cd == 'W' ~ 'White',
        race_cd %in% c('U', 'S') ~ 'Unknown',
        TRUE ~ 'Unknown'
      )
    ) %>%
    dplyr::mutate(date = date_field)



  test2 <- prison %>%
    dplyr::select(ofndr_num,
                  age,
                  lgl_stat_cd,
                  race_type,
                  sex,
                  dob,
                  assgn_dt,
                  body_loc_desc)



  ofndr_num <- glue::glue_sql("{test2$ofndr_num*}", .con = con)
  q1 <-
    glue::glue_sql(
      "select ofndr_num, max(rlse_dt) as release from prsn_rlse where rlse_tm is null and ofndr_num in({`ofndr_num`})
                     group by ofndr_num",
      .con = con
    )
  q1 <- DBI::dbGetQuery(con, q1)

  test2 <- test2 %>%
    dplyr::left_join(q1, by = "ofndr_num")


  q1 <- glue::glue_sql(
    "select distinct ofndr_num, ad_seg
from
(SELECT b.ofndr_num, a.begin_dt, trim(c.ad_seg_typ_desc) as ad_seg, rank() over(partition by ofndr_num order by a.begin_dt desc) as rank
      FROM ad_seg_typ a, (select *, case when end_dt is null then today else end_dt end as end_date from ad_seg) b, ad_seg_typ_cd c
     WHERE b.ofndr_num in ({`ofndr_num`})
       AND b.end_date >DATE('{`sql_date`}') and b.begin_dt<=DATE('{`sql_date`}')
       AND a.ad_seg_id = b.ad_seg_id
       AND a.ad_seg_typ_cd = c.ad_seg_typ_cd
     order by a.begin_dt desc)
     where rank=1",
    .con = con
  )

  q1 <- DBI::dbGetQuery(con, q1)

  test2 <- test2 %>%
    dplyr::left_join(q1, by = "ofndr_num")

  #bed
  q1 <- glue::glue_sql(
    "
select ofndr_num, trim(p_house_sctn_name) as p_house_sctn_name, trim(cell_id) as cell_id, trim(bed_name) as bed_name
from (select *, case when end_dt is null then today else end_dt end as end_date from dio_ofndr_bed) a
join dio_bed b
on a.bed_id=b.bed_id
where ofndr_num in  ({`ofndr_num`})
and a.strt_dt<= DATE('{`sql_date`}') and end_date>DATE('{`sql_date`}')",
    .con = con
  )

  q1 <- DBI::dbGetQuery(con, q1)


  q1$bed_assign <-
    paste0(q1$p_house_sctn_name, " ", q1$cell_id, " ", q1$bed_name)
  q1 <- q1[, c(1, 5)]

  test2 <- test2 %>%
    dplyr::left_join(q1, by = "ofndr_num")

  q1 <-
    glue::glue_sql(
      "select distinct a.ofndr_num, max(b.parole_elig_date) over (partition by ofndr_num) as parole_date
from
(select distinct ofndr_num, max(a.parole_elig_tracking_id) over(partition by ofndr_num) as parole_date
from parole_eligibility_dates a
join parole_eligibility_tracking b
on a.parole_elig_tracking_id=b.parole_elig_tracking_id
where ofndr_num in ({`ofndr_num`})
  AND parole_elig_date_type_cd = 'D'
	   AND (parole_elig_date_stat_cd IS NULL OR parole_elig_date_stat_cd
 = 'O')
)a
join parole_eligibility_dates b
on a.parole_date=b.parole_elig_tracking_id",
      .con = con
    )

  q1 <- DBI::dbGetQuery(con, q1)

  test2 <- test2 %>%
    dplyr::left_join(q1, by = "ofndr_num")


  q1 <-
    glue::glue_sql(
      "select distinct ofndr_num, max(furlgh_elgb_dt) over (partition by ofndr_num) as furlough_date
from ofndr_clfn
where ofndr_num in ({`ofndr_num`})
and furlgh_elgb_dt > DATE('{`sql_date`}')
order by 2",
      .con = con
    )

  q1 <- DBI::dbGetQuery(con, q1)

  test2 <- test2 %>%
    dplyr::left_join(q1, by = "ofndr_num")


  q1 <-
    glue::glue_sql(
      "select distinct ofndr_num, trim(b.clfn_cstdy_title) as custody_level
from (select *, case when end_dt is null then today else end_dt end as end_date from ofndr_clfn) a
join clfn_cstdy_lvl_cd b
on a.base_cstdy_lvl_cd=b.clfn_cstdy_lvl_cd
where ofndr_num in ({`ofndr_num`})
and effective_date<=DATE('{`sql_date`}') and end_date>DATE('{`sql_date`}')",
      .con = con
    )

  q1 <- DBI::dbGetQuery(con, q1)

  test2 <- test2 %>%
    dplyr::left_join(q1, by = "ofndr_num")


  q1 <- glue::glue_sql(
    "select ofndr_num, filing_dt
from dcpln_case
where filing_dt<=DATE('{`sql_date`}') and ofndr_num in ({`ofndr_num`})",
    .con = con
  )

  q1 <- DBI::dbGetQuery(con, q1)

  writeups <- test2 %>%
    dplyr::select(ofndr_num, assgn_dt) %>%
    dplyr::inner_join(q1, by = "ofndr_num") %>%
    dplyr::filter(filing_dt >= assgn_dt)


  writeups$curr_month <-
    ifelse(
      writeups$filing_dt >= as.Date("2022-02-01") &
        writeups$filing_dt <= as.Date("2022-02-28"),
      "TRUE",
      ""
    )

  write_up_cum <- writeups %>%
    dplyr::group_by(ofndr_num) %>%
    dplyr::count(name = "writeups")

  write_up_month <- writeups %>%
    dplyr::group_by(ofndr_num) %>%
    dplyr::filter(curr_month == TRUE) %>%
    dplyr::count(name = "writeups_month")

  test2 <- test2 %>%
    dplyr::left_join(write_up_cum, by = "ofndr_num") %>%
    dplyr::left_join(write_up_month, by = "ofndr_num")


  q1 <-
    glue::glue_sql(
      "SELECT distinct ofndr_num, wpa.wrk_slot_id, wpa.wrk_prog_strt_dt, scrn_dt,
               scrn_dcsn_cd, exit_typ_cd, wpa.end_dt, jt.pay_rate,
               pl.wrk_loc_desc, jt.job_title_desc, wpa.updt_usr_id,
               wpa.updt_dt, wpa.max_hr_wk, wpa.dtl_cmt, adhoc_pay_rate

          FROM (select *, case when end_dt is null then today else end_dt end as end_date from wrk_prog_assign) wpa, wrk_assign_slot was,
               pos_loc_cd pl, job_title jt
         WHERE was.wrk_slot_id = wpa.wrk_slot_id
           AND pl.pos_loc_cd = was.pos_loc_cd
           AND jt.job_title_cd = was.job_title_cd
           and ofndr_num in ({ofndr_num})
          and wpa.wrk_prog_strt_dt<=Date('03/31/2022')
          and end_date>Date('03/31/2022')

         ORDER BY wrk_prog_strt_dt DESC",
      .con = con
    )

  q1 <- DBI::dbGetQuery(con, q1)

  q1 <- q1 %>%
    dplyr::group_by(ofndr_num) %>%
    dplyr::mutate(row = dplyr::row_number()) %>%
    dplyr::filter(row == 1)

  q1 %>%
    dplyr::group_by(ofndr_num) %>%
    dplyr::count(name = "count") %>%
    dplyr::filter(count > 1)

  employed <- q1$ofndr_num

  jobs_report <- test2 %>%
    dplyr::mutate(employed = dplyr::case_when(ofndr_num %in% employed ~
                                                TRUE,
                                              TRUE ~ FALSE)) %>%
    dplyr::select(ofndr_num, employed)

  test2 <- test2 %>%
    dplyr::left_join(jobs_report, by = "ofndr_num")



  DBI::dbDisconnect(con)

  test2$ad_seg <- ifelse(is.na(test2$ad_seg), "", test2$ad_seg)
  test2$writeups <- ifelse(is.na(test2$writeups), 0, test2$writeups)
  test2$writeups_month <-
    ifelse(is.na(test2$writeups_month), 0, test2$writeups_month)
  test2$employed <- ifelse(test2$employed == TRUE, "Yes", "No")


  rpt_name <- paste(md, yt, "report", sep = "_")
  rpt_name <- paste0(rpt_name, ".xlsx")
  sheet_name <- paste(md, yt)

  # xlsx::write.xlsx(test2, rpt_name, sheetName=sheet_name, row.names = FALSE, showNA = FALSE)
  if (excel == TRUE) {
    folder <- paste(md, yt, sep = "-")
    if (file.exists(folder)) {
      cat("Folder Exists")
    } else {
      dir.create(folder)
    }


    list <- unique(test2$body_loc_desc)
    sheet_name <- paste(md, yt)
    excel <- function(x) {
      test2 <- test2 %>%
        dplyr::filter(body_loc_desc == x)
      file <- paste0(x, "_", md, dy, ".xlsx")
      location <- paste(folder, file, sep = "/")
      wb <- openxlsx::createWorkbook()
      openxlsx::addWorksheet(wb, sheet_name)
      openxlsx::writeData(wb, sheet_name, test2)
      openxlsx::saveWorkbook(wb, location, overwrite = TRUE)
    }

    invisible(lapply(list, excel))
  }
  return(test2)
}
