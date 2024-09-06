# install.packages("data.table")
library(data.table)
# install.packages("fastDummies")
library(fastDummies)
# install.packages("forecast")
library(forecast)
# install.packages("tidyverse")
library(tidyverse)
# install.packages("glue")
library(glue)
# install.packages("mgcv")
library(mgcv)
# install.packages("tsibble")
library(tsibble)

old_wd <- getwd()
new_wd <- "~/Documents/R_wd/r_projects/milad/"
setwd(new_wd)

paths <- c(adintel = "A_Processed_For_Regression.csv",
           creatives = "CreativeDescriptions_Coded.csv",
           rms = "r_top40_brands_week.csv",
           brand_matches = "T40_Final_BrandMatch.csv",
           programs = "TV_Program_Categories.csv")

paths <- setNames(file.path(getwd(), paths), names(paths))

tbl_list <- lapply(
  X = paths, 
  FUN = function(x) tibble::as_tibble(data.table::fread(x))
) 

as_index <- function(x) {
  out <- as.factor(x)
  as.integer(out)
}

x <- tbl_list |> 
  getElement("programs") |> 
  rename_with(snakecase::to_snake_case) |> 
  rename(tv_prog_cat_code = tv_prog_cat_id,
         tv_prog_cat_desc = tv_prog_cat) |> 
  mutate(tv_prog_id = cur_group_id(), 
         .by = c(tv_prog_cat_desc, tv_prog_cat_code)) |> 
  select(tv_prog_id, 
         tv_prog_cat_code,
         tv_prog_cat_desc,
         tv_prog_type_code,
         tv_prog_type_desc) |> 
  mutate(tv_prog_code = case_when(
    tv_prog_cat_code == "ENT" ~ "show",
    tv_prog_cat_code == "SPC" ~ "spcl",
    tv_prog_cat_code == "CHD" ~ "chld",
    tv_prog_cat_code == "INF" ~ "educ",
    tv_prog_cat_code == "REL" ~ "relg",
    tv_prog_cat_code == "MISC" ~ "misc",
    tv_prog_cat_code == "SPR" ~ "sprt"
  ),.after = tv_prog_id)

x |> 
  # rename(tv_prog_cat_code0 = tv_prog_cat_code,
  #        tv_prog_type_code0 = tv_prog_type_code) |> 
  mutate(
    # tv_prog_cat_code = tolower(tv_prog_cat_code0),
         tv_prog_cat_code = case_when(tv_prog_cat_code == "ENT" ~ "show",
                                      tv_prog_cat_code == "SPC" ~ "spcl",
                                      tv_prog_cat_code == "CHD" ~ "chld",
                                      tv_prog_cat_code == "INF" ~ "educ",
                                      tv_prog_cat_code == "REL" ~ "relg",
                                      tv_prog_cat_code == "MISC" ~ "misc",
                                      tv_prog_cat_code == "SPR" ~ "sprt"),
         .after = tv_prog_type_code)

x |> 
  select(-tv_prog_id) |> 
  decompose_table(tv_prog_id,
                  tv_prog_code,
                  tv_prog_cat_code,
                  tv_prog_cat_desc)

tv_prog_type <- select(x, 
                       tv_prog_id,
                       tv_prog_code,
                       tv_prog_type_code,
                       tv_prog_type_desc)
tv_prog_cat <- select(x, 
                       tv_prog_id,
                      tv_prog_code,
                       tv_prog_cat_code,
                       tv_prog_cat_desc)
install.packages("dm")
library(dm)

dm::decompose_table(select(x, -tv_prog_id), tv_prog_id, tv_prog_type_code, tv_prog_type_desc)

tv_prog <- select(x)

x |> 
  nest(tv_prog_type = c(tv_prog_type_code, tv_prog_type_desc),
       tv_prog_cat = c(tv_prog_cat_code, tv_prog_cat_desc))
x  

grou
  nest(.by = c(TVProgCat, TVProgCatID))
x |> 
  mutate(prog_id = tolower(TVProgCatID),
         prog_id = case_when(prog_id == "ent" ~ "show",
                             prog_id == "spc" ~ "spcl",
                             prog_id == "chd" ~ "chld",
                             prog_id == "inf" ~ "educ",
                             prog_id == "rel" ~ "relg",
                             prog_id == "misc" ~ "misc",
                             prog_id == "spr" ~ "sprt")) |> 
  relocate(prog_id, .before = 1) |>
  nest(.by = prog_id) |> 
  deframe() |> 
  lapply(unnest, everything())
  



|> 
enframe(name = "old_name", value = "tbl") |> 
  mutate(name = c("adintel",
                  "creatives",
                  "rms",
                  "brand_matches",
                  "programs"),
         .before = 1) |> 
  select(-old_name) |> 
  deframe()
  


lapply(paths, function(x) tibble::as_tibble(data.table::fread(x)))
  

# final_match_dir <- "D:/My_Codes/Thesis/final_match/"
final_match_dir <- "~/Documents/R_wd/r_projects/milad/"
# Preprocessing ------------------------------------------------------------
# Read data pre-processed for regression
input_file <- file.path(final_match_dir, "A_Processed_For_Regression.csv")
df <- fread(file = input_file) |>
  mutate(week_end = ymd(week_end))

# Create dummy variables for TVProgCatID and CreativeType
df <- df |> dummy_cols(
  select_columns = "TVProgCatID",
  remove_first_dummy = TRUE,
  omit_colname_prefix = TRUE
) |>
  dummy_cols(
    select_columns = "CreativeType",
    remove_first_dummy = TRUE,
    omit_colname_prefix = TRUE
  )
df

# AdIntel - RMS matched brands
input_file_matches <- file.path(final_match_dir, "T40_Final_BrandMatch.csv")
matches <- fread(file = input_file_matches) |>
  distinct(brand_code_uc, BrandCode)

input_file_rms <- file.path(final_match_dir, "r_top40_brands_week.csv")
rms <- fread(file = input_file_rms) |>
  mutate(week_end = mdy(week_end)) |>
  select(-brand_descr)

df <- df |>
  inner_join(matches, by = "BrandCode") |> 
  merge(rms, by = c("brand_code_uc", "week_end"), all = FALSE)

# df <- df |>
#   merge(rms, by = c("brand_code_uc", "week_end"), all = FALSE)


# Aggregate data to week level. We use sum for GRP and Spend.
# For categorical variables we use both total and mean.
# We also create sinousoidal features for time which is week_end here.

df_weekly <- df |>
  group_by(brand_code_uc, week_end) |>
  summarize(
    Spend = sum(Spend, na.rm = TRUE),
    GRP = sum(GRP, na.rm = TRUE),
    Revenue = first(revenue),
    inf_avg = mean(Informational, na.rm = TRUE),
    ENT_avg = mean(ENT, na.rm = TRUE),
    INF_avg = mean(INF, na.rm = TRUE),
    SPC_avg = mean(SPC, na.rm = TRUE),
    SPR_avg = mean(SPR, na.rm = TRUE),
    inf_total = sum(Informational, na.rm = TRUE),
    ENT_total = sum(ENT, na.rm = TRUE),
    INF_total = sum(INF, na.rm = TRUE),
    SPC_total = sum(SPC, na.rm = TRUE),
    SPR_total = sum(SPR, na.rm = TRUE),
    inf_ENT_avg = mean(Informational * ENT, na.rm = TRUE),
    inf_INF_avg = mean(Informational * INF, na.rm = TRUE),
    inf_SPC_avg = mean(Informational * SPC, na.rm = TRUE),
    inf_SPR_avg = mean(Informational * SPR, na.rm = TRUE),
    inf_ENT_total = sum(Informational * ENT, na.rm = TRUE),
    inf_INF_total = sum(Informational * INF, na.rm = TRUE),
    inf_SPC_total = sum(Informational * SPC, na.rm = TRUE),
    inf_SPR_total = sum(Informational * SPR, na.rm = TRUE),
    .groups = "keep"
  ) |>
  ungroup() |>
  mutate(
    ad_month = month(week_end),
    ad_week = week(week_end),
    week_sin = sin(2 * pi * ad_week / 52),
    week_cos = cos(2 * pi * ad_week / 52)
  )

df_weekly |> summary()

x <- as_tibble(df_weekly) 
x |> 
  mutate(t = tsibble::yearweek(week_end),
         i = as.integer(as_factor(brand_code_uc)),
         .before = 1) |> 
  as_tsibble(key = i, index = t) |> 
  fill_gaps()  |> 
  transmute(Revenue,
            adv
            adv_inf = GRP * inf_avg,
            adv_emo = GRP * (1 - inf_avg))
  # fill_gaps(.full = start())  
  
x |> 
  as_tibble() |> 
  mutate(t = str_remove_all(t, regex("^[0-9]{4}\\sW")) |> 
           as.integer()) |> 
  summary()

|> 
  ummary()
   |> 
  group_by(i) |> 
  summarise(n = sum(!is.na(Revenue)))




  group_by(brand_code_uc) |> 
  group_data()




# Finding optimal lambda for GRP and Revenue
# used for Box-Cox transformation
optimal_lambda_GRP <- BoxCox.lambda(df_weekly$GRP)
optimal_lambda_Revenue <- BoxCox.lambda(df_weekly$Revenue)

# Apply Box-Cox and log transformation to GRP and Revenue
df_weekly <- df_weekly |>
  mutate(
    GRP_log = log(GRP + 1),
    GRP_boxcox = BoxCox(GRP, optimal_lambda_GRP),
    Revenue_log = log(Revenue + 1),
    Revenue_boxcox = BoxCox(Revenue, optimal_lambda_Revenue)
  )


# Model 1. Base Model ----------------------------------------------------------
model_1 <- lm(
  Revenue_boxcox ~
    GRP_boxcox +
    inf_avg +
    inf_ENT_avg +
    inf_INF_avg +
    inf_SPC_avg +
    inf_SPR_avg +
    0,
  data = df_weekly
)

summary(model_1)
options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)
# Set up a 2x2 plotting area
par(mfrow = c(3, 2))

# Plot diagnostic plots
plot(model_1)

residuals <- residuals(model_1)
fitted_values <- fitted(model_1)

# ACF plot of residuals
acf(residuals, main = "ACF of Residuals")

# Histogram of residuals
hist(residuals, main = "Histogram of Residuals", breaks = 30, col = "lightblue")


# Model 2.Linear Regression with lagged variables ------------------------------
model_2 <- lm(
  Revenue_boxcox ~
    lag(GRP_boxcox, 3) +
    lag(GRP_boxcox, 4) +
    lag(inf_avg, 3) +
    lag(inf_avg, 4) +
    lag(inf_ENT_avg, 3) +
    lag(inf_INF_avg, 3) +
    lag(inf_SPC_avg, 3) +
    lag(inf_SPR_avg, 3) +
    lag(inf_ENT_avg, 4) +
    lag(inf_INF_avg, 4) +
    lag(inf_SPC_avg, 4) +
    lag(inf_SPR_avg, 4) +
    0,
  data = df_weekly
)

summary(model_2)

options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)
# Set up a 2x2 plotting area
par(mfrow = c(3, 2))

# Plot diagnostic plots
plot(model_2)

residuals <- resid(model_2)
fitted_values <- fitted(model_2)

# ACF plot of residuals
acf(residuals, main = "ACF of Residuals")

# Histogram of residuals
hist(residuals, main = "Histogram of Residuals", breaks = 30, col = "lightblue")

mtext("Model 2.2: LM Lagged Variables", side = 3, line = - 2, outer = TRUE)

# Model 3. AR(1) Model ---------------------------------------------------
gls_ar1 <- gls(
  Revenue_boxcox ~
    GRP_boxcox +
    inf_avg +
    inf_ENT_avg +
    inf_INF_avg +
    inf_SPC_avg +
    inf_SPR_avg +
    0,
  data = df_weekly,
  correlation = corARMA(p = 1, q = 0, form = ~ 1 | week_end),
  weights = varIdent(form = ~ 1 | brand_code_uc)
)

summary(gls_ar1)

options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)

par(mfrow = c(3, 2))

# Residuals and Fitted values
residuals <- resid(gls_ar1)
fitted_values <- fitted(gls_ar1)


# Q-Q plot for normality
qqnorm(residuals)
qqline(residuals, col = "red")

# Histogram of residuals
hist(residuals, breaks = 50, main = "Histogram of Residuals", xlab = "Residuals")

# Residuals vs Fitted values plot for homoscedasticity
plot(fitted_values, residuals)
abline(h = 0, col = "red")

acf(residuals, main = "ACF of Residuals")  # ACF plot
pacf(residuals, main = "PACF of Residuals")  # PACF plot



# Model 4. GLS ARMA(2,2) ---------------------------------------------------
gls_arma22 <- gls(
  Revenue_log ~
    GRP_log +
    inf_avg +
    inf_ENT_avg +
    inf_INF_avg +
    inf_SPC_avg +
    inf_SPR_avg,
  data = df_weekly,
  correlation = corARMA(p = 2, q = 2, form = ~ 1 | week_end),
  weights = varIdent(form = ~ 1 | brand_code_uc)
)

# Summary of the AR(2) model
summary(gls_arma22)

options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)

par(mfrow = c(3, 2))

# Residuals and Fitted values
residuals <- resid(gls_arma22)
fitted_values <- fitted(gls_arma22)

# Q-Q plot for normality
qqnorm(residuals)
qqline(residuals, col = "red")

# Histogram of residuals
hist(residuals, breaks = 50, main = "Histogram of Residuals", xlab = "Residuals")

# Residuals vs Fitted values plot for homoscedasticity
plot(fitted_values, residuals)
abline(h = 0, col = "red")

acf(residuals, main = "ACF of Residuals")  # ACF plot
pacf(residuals, main = "PACF of Residuals")  # PACF plot


# Model 5. GAM -------------------------------------------------------------
gam_model <- gam(
  Revenue_log ~
    s(GRP_log) +
    s(inf_avg) +
    te(inf_avg, ENT_avg) +
    te(inf_avg, INF_avg) +
    te(inf_avg, SPC_avg) +
    te(inf_avg, SPR_avg) +
    s(lag(GRP_log, 1)) +
    s(lag(inf_avg, 1)) +
    te(lag(inf_avg, 1), lag(ENT_avg, 1)) +
    te(lag(inf_avg, 1), lag(INF_avg, 1)) +
    te(lag(inf_avg, 1), lag(SPC_avg, 1)) +
    te(lag(inf_avg, 1), lag(SPR_avg, 1)) +
    te(lag(inf_avg, 2), lag(ENT_avg, 2)) +
    te(lag(inf_avg, 2), lag(INF_avg, 2)) +
    te(lag(inf_avg, 2), lag(SPC_avg, 2)) +
    te(lag(inf_avg, 2), lag(SPR_avg, 2)), 
  data = df_weekly
)
summary(gam_model)

options(
  repr.plot.res = 200,
  repr.plot.height = 8,
  repr.plot.width = 6
)

par(mfrow = c(3, 2))

plot(gam_model)
options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)

par(mfrow = c(3, 2))

# Residuals and Fitted values
residuals <- resid(gam_model)
fitted_values <- fitted(gam_model)

# Q-Q plot for normality
qqnorm(residuals)
qqline(residuals, col = "red")

# Histogram of residuals
hist(residuals, breaks = 50, main = "Histogram of Residuals", xlab = "Residuals")

# Residuals vs Fitted values plot for homoscedasticity
plot(fitted_values, residuals)
abline(h = 0, col = "red")

acf(residuals, main = "ACF of Residuals")  # ACF plot
pacf(residuals, main = "PACF of Residuals")  # PACF plot

# Model 6. DLM -------------------------------------------------------------
library(dlm)
df_weekly <- df_weekly |>
  mutate(
    inf_INF = inf_avg * INF_avg,
    inf_SPC = inf_avg * SPC_avg,
    inf_SPR = inf_avg * SPR_avg,
    inf_ENT = inf_avg * ENT_avg,
    inf_INF_lag1 = lag(inf_INF_avg, 1),
    inf_SPC_lag1 = lag(inf_SPC_avg, 1),
    inf_SPR_lag1 = lag(inf_SPR_avg, 1),
    inf_ENT_lag1 = lag(inf_ENT_avg, 1),
    inf_ENT_lag2 = lag(inf_ENT_avg, 2),
    inf_INF_lag2 = lag(inf_INF_avg, 2),
    inf_SPC_lag2 = lag(inf_SPC_avg, 2),
    inf_SPR_lag2 = lag(inf_SPR_avg, 2),
    GRP_lag1 = lag(GRP, 1),
    GRP_lag2 = lag(GRP, 2)
  ) |>
  drop_na(
    inf_INF,
    inf_SPC,
    inf_SPR,
    inf_ENT,
    inf_INF_lag1,
    inf_SPC_lag1,
    inf_SPR_lag1,
    inf_ENT_lag1,
    inf_ENT_lag2,
    inf_INF_lag2,
    inf_SPC_lag2,
    inf_SPR_lag2,
    GRP_lag1,
    GRP_lag2
  )

names(df_weekly)
# Example time series data
revenue_ts <- ts(df_weekly$Revenue, frequency = 52) # Assuming weekly data

df_X <- df_weekly |>
  select(
    GRP,
    inf_avg,
    inf_ENT_avg,
    inf_INF_avg,
    inf_SPC_avg,
    inf_SPR_avg
  )

# Define a local level model with regressors
model_dlm <- dlmModPoly(order = 1, dV = 1, dW = 1) + # Local level model
  dlmModReg(X = df_X, addInt = FALSE)

# Fit the model to the revenue time series data
fit_dlm <- dlmMLE(
  revenue_ts,
  parm = log(c(1, 1, rep(0.1, ncol(df_X)))),
  build = function(parm) {
    model_dlm$V[] <- exp(parm[1]) # Observation noise
    model_dlm$W[] <- exp(parm[2:length(parm)]) + 1e-5 # State noise
    return(model_dlm)
  }
)

# Use the optimized parameters to update the model
model_dlm_fitted <- model_dlm
model_dlm_fitted$V[] <- exp(fit_dlm$par[1])
model_dlm_fitted$W[] <- exp(fit_dlm$par[2:length(fit_dlm$par)])

# Filtering (Kalman Filter) to get the state estimates
filtered_dlm <- dlmFilter(revenue_ts, model_dlm_fitted)

# Smoothing (Kalman Smoother) to get the smoothed state estimates
smoothed_dlm <- dlmSmooth(filtered_dlm)

# Extract variable names from df_X
state_names <- colnames(df_X)

options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)

par(mfrow = c(3, 2))
for (i in 1:length(state_names)) {
  plot(
    filtered_dlm$m[, i],
    type = "l",
    main = paste("Filtered State -", state_names[i])
  )
}

# Plot smoothed states with custom labels
par(mfrow = c(3, 2)) # Adjust layout depending on the number of plots
for (i in 1:length(state_names)) {
  plot(
    smoothed_dlm$s[, i],
    type = "l",
    main = paste("Smoothed State -", state_names[i])
  )
}


# Example time series data
revenue_ts <- ts(df_weekly$Revenue, frequency = 52) # Assuming weekly data

df_X <- df_weekly |>
  select(
    GRP,
    inf_avg,
    ENT_avg,
    INF_avg,
    SPC_avg,
    SPR_avg
  )

# Define a local level model with regressors
model_dlm <- dlmModPoly(order = 1, dV = 1, dW = 1) + # Local level model
  dlmModReg(X = df_X, addInt = FALSE)

# Fit the model to the revenue time series data
fit_dlm <- dlmMLE(
  revenue_ts,
  parm = log(c(1, 1, rep(0.1, ncol(df_X)))),
  build = function(parm) {
    model_dlm$V[] <- exp(parm[1]) # Observation noise
    model_dlm$W[] <- exp(parm[2:length(parm)]) + 1e-5 # State noise
    return(model_dlm)
  }
)

# Use the optimized parameters to update the model
model_dlm_fitted <- model_dlm
model_dlm_fitted$V[] <- exp(fit_dlm$par[1])
model_dlm_fitted$W[] <- exp(fit_dlm$par[2:length(fit_dlm$par)])

# Filtering (Kalman Filter) to get the state estimates
filtered_dlm <- dlmFilter(revenue_ts, model_dlm_fitted)

# Smoothing (Kalman Smoother) to get the smoothed state estimates
smoothed_dlm <- dlmSmooth(filtered_dlm)

# Extract variable names from df_X
state_names <- colnames(df_X)

options(repr.plot.res = 200, repr.plot.height = 8, repr.plot.width = 6)

par(mfrow = c(3, 2))
for (i in 1:length(state_names)) {
  plot(
    filtered_dlm$m[, i],
    type = "l",
    main = paste("Filtered State -", state_names[i])
  )
}

# Plot smoothed states with custom labels
par(mfrow = c(3, 2)) # Adjust layout depending on the number of plots
for (i in 1:length(state_names)) {
  plot(
    smoothed_dlm$s[, i],
    type = "l",
    main = paste("Smoothed State -", state_names[i])
  )
}
