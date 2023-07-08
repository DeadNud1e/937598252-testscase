library(data.table)
library(doParallel)
library(lubridate)
library(ggplot2)
library(dplyr)
library(stringr)
library(tidyverse)


dataset <- fread("dataset.csv")


n_cores <- detectCores() -1 

cl <- makeCluster(n_cores)
registerDoParallel(cl)

# Разбиваем данные на части
parts <- split(dataset, seq_len(n_cores))

# Обрабатываем данные параллельно
results <- foreach(i = seq_along(parts), .combine = rbind, .packages = c("data.table", "lubridate")) %dopar% {
  part <- parts[[i]]
  
  # Удаляем NA
  part <- na.omit(part)
  
  # Удаляем дубликаты
  part <- unique(part)
  
  # Удаляем записи от 1 до 3 часов ночи
  part <- part[!(hour(part$datetime_col) >= 1 & hour(part$datetime_col) <= 3), ]
  
  return(part)
}

stopCluster(cl)

#fwrite(results, "processed_dataset.csv")

# Добавляем столбцы для года, месяца и дня
results$year <- year(results$datetime_col)
results$month <- month(results$datetime_col)
results$day <- day(results$datetime_col)
results$hour <- hour(results$datetime_col)

# Группируем по году, месяцу, дню и часу, затем рассчитываем метрики
aggregated <- results[, .(unique_string = length(unique(string_col)),
                          mean_numeric1 = mean(numeric_col1, na.rm = TRUE),
                          median_numeric1 = median(numeric_col1, na.rm = TRUE) + 0.0),
                      by = .(year, month, day, hour)]


# Присоединяем агрегированные метрики к исходному датасету
results <- merge(results, aggregated, by = c("year", "month", "day", "hour"), all.x = TRUE)

# SQL

# SELECT 
# EXTRACT(YEAR FROM datetime_col) AS year, 
# EXTRACT(MONTH FROM datetime_col) AS month, 
# EXTRACT(DAY FROM datetime_col) AS day, 
# EXTRACT(HOUR FROM datetime_col) AS hour,
# COUNT(DISTINCT string_col) AS unique_string, 
# AVG(numeric_col1) AS mean_numeric1,
# PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY numeric_col1) AS median_numeric1
# FROM results
# GROUP BY year, month, day, hour

#fwrite(results, "final_dataset.csv")

dataset <- results


ggplot(dataset, aes(x = numeric_col1)) +
  geom_histogram(binwidth = 1, fill = "blue", color = "black") +
  labs(title = "Гистограмма для numeric_col1",
       x = "numeric_col1",
       y = "Частота") +
  theme_minimal()

# Расчет 95% доверительного интервала
conf_int <- dataset %>%
  summarise(mean = mean(numeric_col1, na.rm = TRUE),
            sd = sd(numeric_col1, na.rm = TRUE),
            n = n(),
            se = sd / sqrt(n)) %>%
  mutate(lower = mean - qnorm(0.975) * se,
         upper = mean + qnorm(0.975) * se)

print(conf_int)

# Если у нас большой объем данных, то центральная предельная теорема говорит нам, 
# что распределение средних значений будет приближаться к нормальному, 
# вне зависимости от формы исходного распределения данных. В этом случае мы можем использовать z-оценку
# для расчета доверительного интервала.



# Преобразование даты в формат даты R
dataset$datetime_col <- as.POSIXct(dataset$datetime_col)

# Создание новой колонки "месяц"
dataset$year_month <- format(dataset$datetime_col, "%Y-%m")

# Расчет среднего значения numeric_col1 по месяцам
avg_data <- dataset[, .(avg = mean(numeric_col1, na.rm = TRUE)), by = year_month]

# Построение графика
ggplot(avg_data, aes(x = year_month, y = avg)) +
  geom_col(fill = "skyblue") +
  labs(x = "Месяц", y = "Среднее значение numeric_col1", title = "Среднее значение numeric_col1 по месяцам") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))



# Преобразование строки в список символов с привязкой к году-месяцу
chars <- dataset %>% 
  mutate(chars = strsplit(as.character(string_col), "")) %>%
  unnest(chars)

# Подсчет частоты каждого символа для каждого месяца
char_freq <- chars %>% count(month, chars)

# Переименование столбцов для удобства
names(char_freq) <- c("Month", "Character", "Frequency")

# Преобразование датафрейма в "широкий" формат для тепловой карты
char_freq_wide <- char_freq %>% pivot_wider(names_from = Character, values_from = Frequency)

# Построение тепловой карты
ggplot(char_freq, aes(x = Month, y = Character, fill = Frequency)) +
  geom_tile() +
  theme(axis.text.x = element_text(angle = 90)) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  labs(title = "Heatmap of Character Frequency by Month")


#Доп задание 1
set.seed(123)  # для воспроизводимости

# создаем вектор индексов
indexes <- sample(1:nrow(dataset), nrow(dataset))

# размеры разбиений
sizes <- c(0.25, 0.25, 0.5) * nrow(dataset)

# индексы для разбиений
idx1 <- indexes[1:sizes[1]]
idx2 <- indexes[(sizes[1]+1):(sizes[1]+sizes[2])]
idx3 <- indexes[(sizes[1]+sizes[2]+1):nrow(dataset)]

part1 <- dataset[idx1,]
part2 <- dataset[idx2,]
part3 <- dataset[idx3,]

part1$group <- "Group1"
part2$group <- "Group2"
part3$group <- "Group3"

# Объединяем датасеты
combined_data <- rbind(part1, part2, part3)

# ANOVA
# Если бы у нас было только две группы с небольшим количеством данных для сравнения, мы могли бы использовать t-тест. 
# Однако, у нас более 2х групп и много данных, поэтому ANOVA является подходящим инструментом. 
# Если ANOVA показывает, что есть значимое различие между группами, то дополнительные тесты (например, тест Тьюки)
# можно использовать для определения, между какими группами существуют эти различия.
# 
# Помимо этого, ANOVA применима только для непрерывных (количественных) переменных,
# Поскольку наша переменная numeric_col1 является непрерывной, ANOVA является подходящим выбором.

fit <- aov(numeric_col1 ~ group, data = combined_data)
summary(fit)
# Интерпретировать результаты ANOVA можно следующим образом: если p-value (Pr(>F)) получается меньше 0.05, это означает,
# что существуют статистически значимые различия между средними значениями групп.

# Подготавливаем ANOVA таблицу
anova_table <- summary(fit)

# Расчет эффекта eta-квадрат
effect_size <- anova_table[[1]]["group", "Sum Sq"] / sum(anova_table[[1]][, "Sum Sq"])

print(effect_size)

# Размер эффекта (eta-квадрат): Этот показатель используется для количественной оценки степени влияния группы на переменную numeric_col1.
# Размер эффекта может быть полезен для определения, насколько "сильным" или "значительным" является обнаруженный эффект, 
# вне зависимости от статистической значимости. Eta-квадрат в частности выбран для оценки размера эффекта в ANOVA,
# поскольку он представляет собой долю общей вариабельности, объясняемую фактором группы.


# Доп задание 2
# Если мы предполагаем, что вероятность успеха для любого прототипа игры одинакова и не зависит от компании,
# мы можем оценить эту вероятность, используя данные от конкурента.
# 
# Таким образом, вероятность успеха одного прототипа P = 5 / 1000 = 0.005.
# 
# По закону Бернулли, вероятность, что следующий (201й) прототип будет успешен, равна вероятности успеха одного прототипа, 
# то есть 0.005 (или 0.5%).
