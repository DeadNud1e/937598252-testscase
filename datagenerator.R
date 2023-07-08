library(data.table)

n_rows <- 1e6  # 1e8 для 100 миллионов строк

# Функция для генерации даты
random_dates <- function(n, start_date = "2000-01-01 00:00:00", end_date = "2020-12-31 23:59:59"){
  start_date <- as.POSIXct(start_date, tz = "UTC")
  end_date <- as.POSIXct(end_date, tz = "UTC")
  random_dates <- as.POSIXct(runif(n, min = start_date, max = end_date), origin="1970-01-01")
  return(random_dates)
}

# Создание датасета
dataset <- data.table(
  numeric_col1 = round(runif(n_rows, min=-10, max=15), 0), #runif(n_rows),
  datetime_col = random_dates(n_rows),
  string_col = sample(letters, n_rows, replace = TRUE)
)

# Дубликаты и NA
set.seed(123) # для воспроизводимости

dup_rows <- sample(1:n_rows, size = n_rows * 0.10, replace = TRUE) # выбираем 10% строк для дублирования
na_rows <- sample(1:n_rows, size = n_rows * 0.05, replace = TRUE) # выбираем 5% строк для NA

# Добавляем дублированные строки
dataset <- rbind(dataset, dataset[dup_rows, ])

# Заменяем строки на NA
dataset[na_rows, ] <- NA

fwrite(dataset, "dataset.csv")
