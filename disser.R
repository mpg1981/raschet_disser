#  Схема расчётов:
#
#          БАЗА ДАННЫХ
#              |
#              v
#          01 все станции
#              |
#              v
#          02 биотопы, месяц
#              |
#              v 
#          03 биотопы
#              | 
#              v 
#          04 реки
#              | 
#              v 
#          05 ИТОГО
# 
#_______________________________
#
#         Первичная загрузка данных
#
#_______________________________
#install.packages("RODBC")
library(RODBC)
dta <- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};
                         DBQ=C:/INFOR/Работа/2018/EASTUARY.accdb")
TRLN <- sqlFetch(dta, "01_tsh", as.is = TRUE)
ULOV <- sqlFetch(dta, "02_tsp", as.is = TRUE)
COD <- sqlFetch(dta, "08_codovayaTablicha", as.is = TRUE)
SYSTEMATIKA <- sqlFetch(dta, "09_sistematica", as.is = TRUE)
odbcCloseAll()
rm(dta)
detach(package : RODBC, unload = T)  # выгрузка пакета

library(xlsx)
mnojBiotopi <-  read.xlsx(file = file.choose(),
                          sheetIndex = 1,
                          encoding = "UTF-8",
                          colClasses = NA,
                          stringsAsFactors = FALSE)
BIOTOP <-  read.xlsx(file = file.choose(),
                     sheetIndex = 1,
                     encoding = "UTF-8",
                     colClasses = NA,
                     stringsAsFactors = FALSE)
REKI <-  read.xlsx(file = file.choose(),
                   sheetIndex = 1,
                   encoding = "UTF-8",
                   colClasses = NA,
                   stringsAsFactors = FALSE)

detach(package : xlsx, unload = T)  # выгрузка пакета

#_______________________________
#
#          Первичная обработка данных
#
#_______________________________
library(dplyr)

ulov <- ULOV
ulov$gruppa <- as.numeric(ulov$gruppa)
str(ulov)

#   превращаем красноперок в один вид
krasnop <- c("15322600", "15322601", "15322602")
ulov2 <- ulov %>%
  filter(cod1 %in% krasnop) %>%
  group_by(Key, Stanchiya) %>%
  summarise(
    dlinaMin = min(dlinaMin, na.rm = TRUE),
    dlinaMax = max(dlinaMax, na.rm = TRUE),
    ulovIND = sum(ulovIND, na.rm = TRUE),
    ulovGR = sum(ulovGR, na.rm = TRUE),
    massaPromeraGR = sum(massaPromeraGR, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    cod1 = 15322600,
    gruppa = 0
  )

ulov2$dlinaMin[is.infinite(ulov2$dlinaMin)] <- NA # вместо NA делаем "0"
ulov2$dlinaMax[is.infinite(ulov2$dlinaMax)] <- NA
ulov2$massaPromeraGR[ulov2$massaPromeraGR == 0] <- NA

ulov3 <- ulov %>%
  filter(!(cod1 %in% krasnop)) 
ulov <- ulov3 %>%
  bind_rows(ulov2)
rm(list = c("ulov2", "ulov3", "krasnop"))
#
#_______________________________
#
#          Расчёты
#
#_______________________________
#
#
#        01 - все станции
#
# 32/25, 26/8, 26/9, 61/62, 61/63 -- пустые траления
# 32/25 пустое траление в р. Ольга !!!

promTrali <- c(56, 57, 58, 60, 61, 62, 63, 71) # пром траления

t_01 <- TRLN[, c("Key", "Stanchiya", "biotop", "dataVremyRabot",
                 "sZakidNevoda", "X", "Y")] %>%
  filter(!Key %in% promTrali) %>% #искл. траления по зимовальным ямам на Раздольной
  filter(!(Key == 82 & Stanchiya == 17)) %>% # траление на нерестилище гольяна
  mutate(
    dataVremyRabot = as.POSIXct(dataVremyRabot, "UTC", format = "%Y-%m-%d %H:%M:%S")
  )
rm(promTrali)

t_01_2 <- t_01[ ,c("Key", "Stanchiya", "dataVremyRabot")] %>%
  group_by(Key) %>%
  summarise(
    year = min(as.integer(format(as.Date(dataVremyRabot),"%Y"))),
    month = min(as.integer(format(as.Date(dataVremyRabot),"%m"))),
    vremaRabot = paste(year, "-", month, "-01", sep = "")
  ) %>%
  ungroup() %>%
  mutate(
    vremaRabot = as.Date(vremaRabot, format = "%Y-%m-%d")
  ) # иногда начинают и заканчивают в разные месяцы/года

t_01 <- t_01 %>%
  right_join(t_01_2, by = "Key") %>%
  select(-dataVremyRabot, Strala = sZakidNevoda) %>%
  left_join(BIOTOP[ , c("biotop", "nazvBiotopa", "reka",
                        "nazvReki", "sBiotopa")], by = "biotop") %>%
  filter(month %in% c(5:10)) # только тёплый период

rm(t_01_2)

# работаем с уловами
u_01 <- ulov %>%
  select(Key, Stanchiya, cod1, gruppa, pieces = ulovIND, gramm = ulovGR) %>%
  filter(cod1 != 84870000) %>% # Мизиды не нужны
  filter(cod1 != 38100000) %>% # Класс Гастроподы не нужны
  filter(cod1 != 37000000) %>% # Isopoda не нужны
  filter(cod1 != 37100000) %>% # Amphipoda не нужны
  right_join(t_01, by = c("Key", "Stanchiya")) # пустые траления

u_01[is.na(u_01)] <- 0 # вместо NA делаем "0"

u_01 <- u_01 %>%
  group_by(Key, Stanchiya, cod1, reka, nazvReki,
           biotop, nazvBiotopa, vremaRabot, month, Strala) %>%
  summarise(
    pieces = sum(pieces), # избавляемся от групкода
    gramm = sum(gramm)
  ) %>%
  ungroup() %>%
  mutate(
    pNAm2 = pieces / Strala,
    gNAm2 = gramm / Strala,
    pNAm2 = round(pNAm2, 4),
    gNAm2 = round(gNAm2, 4)
  ) %>%
  select(-Strala) %>%
  left_join(COD[ , c("cod1", "vid")], by = "cod1")

u_01$vid[is.na(u_01$vid)] <- "нет улова" # вместо NA делаем "нет улова"

# все станции итого
u_01_itogo <- u_01 %>%
  group_by(Key, Stanchiya, vremaRabot, month, reka, nazvReki, biotop, nazvBiotopa) %>%
  summarise(
    pNAm2_01 = sum(pNAm2),
    gNAm2_01 = sum(gNAm2),
    PIECES_01 = sum(pieces),
    GRAMM_01 = sum(gramm)
  ) %>%
  ungroup() # ПРОВЕРИТЬ!!!!!!!!!! Кол-во тралов/кол-во станций, нулевые уловы

ifelse(nrow(t_01) - nrow(u_01_itogo) == 0, "Х О Р О Ш О", "!!!!!!!!!!!!!! error !!!!!!!!!!!!!!")

###
#
#        02 - биотопы, месяц
#
t_02 <- t_01 %>%
  group_by(month, reka, nazvReki, biotop, nazvBiotopa, sBiotopa) %>%
  summarise(
    protralili.raz_02 = n(),
    S.oblova_02 = sum(Strala)
  ) %>%
  ungroup() 

u_02 <- u_01 %>%
  group_by(month, biotop, cod1, vid) %>%
  summarise(
    pieces_02 = sum(pieces),
    gramm_02 = sum(gramm),
    poimali.raz_02 = n()
  ) %>%
  ungroup() %>%
  right_join(t_02, by = c("month", "biotop")) %>%
  mutate(
    pNAm2_02 = round(pieces_02 / S.oblova_02, 4),
    gNAm2_02 = round(gramm_02 / S.oblova_02, 5),
    procent_02 = round(poimali.raz_02 * 100 / protralili.raz_02, 1),
    PIECES_02 = round(pNAm2_02 * sBiotopa * 1000000, 0), # перевод в абс. значения
    GRAMM_02 = round(gNAm2_02 * sBiotopa * 1000000, 0)   # для дальнейших расчётов
  ) %>%
  select(-pieces_02, -gramm_02, -protralili.raz_02, -sBiotopa, -S.oblova_02)

# все станции биотоп итого
u_02_itogo <- u_02 %>%
  group_by(month, reka, nazvReki, biotop, nazvBiotopa) %>%
  summarise(
    pNAm2_02 = sum(pNAm2_02),
    gNAm2_02 = sum(gNAm2_02),
    PIECES_02 = sum(PIECES_02), # абсолютное ШТУК на всём биотопе
    GRAMM_02 = sum(GRAMM_02) # абсолютное ГРАММ на всём биотопе
  ) %>%
  ungroup() %>%
  mutate(
    pNAm2_02 = round(pNAm2_02, 2),
    gNAm2_02 = round(gNAm2_02, 2)
  )

###
#
#        03 - биотопы
#
t_03 <- t_02 %>%
  group_by(reka, nazvReki, biotop, nazvBiotopa, sBiotopa) %>%
  summarise(
    protralili.raz_03 = sum(protralili.raz_02),
    kolvo.semok = n() # для ср арифметического
  ) %>%
  ungroup()

u_03 <- u_02 %>%
  group_by(biotop, cod1, vid) %>%
  summarise(
    poimali.raz_03 = sum(poimali.raz_02),
    pNAm2_03 = sum(pNAm2_02),
    gNAm2_03 = sum(gNAm2_02),
    PIECES_03 = sum(PIECES_02),
    GRAMM_03 = sum(GRAMM_02)    # для ср арифметического
  ) %>%
  ungroup() %>%
  left_join(t_03, by = "biotop") %>%
  mutate(
    pNAm2_03 = round(pNAm2_03 / kolvo.semok, 5),
    gNAm2_03 = round(gNAm2_03 / kolvo.semok, 5),
    procent_03 = round(poimali.raz_03 * 100 / protralili.raz_03, 1),
    PIECES_03 = round(PIECES_03 / kolvo.semok, 0),       # ср арифметическое
    GRAMM_03 = round(GRAMM_03 / kolvo.semok, 0)           # абсолют значения
  ) %>%
  select(-protralili.raz_03, -sBiotopa, -protralili.raz_03, -kolvo.semok) %>%
  filter(cod1 > 0) # удаляем пустые траления и уже не нужные

# биотопы итого
u_03_itogo <- u_03 %>%
  group_by(reka, nazvReki, biotop, nazvBiotopa) %>%
  summarise(
    PIECES_03 = sum(PIECES_03),
    GRAMM_03 = sum(GRAMM_03),
    pNAm2_03 = sum(pNAm2_03),
    gNAm2_03 = sum(gNAm2_03),
    kol_vo_vidov = n()
  ) %>%
  ungroup() %>%
  mutate(
    pNAm2_03 = round(pNAm2_03, 1),
    gNAm2_03 = round(gNAm2_03, 1)
  )

###
#
#        04 - реки
#
t_04 <- t_03 %>%
  group_by(reka, nazvReki) %>%
  summarise(
    protralili.raz_04 = sum(protralili.raz_03),
    S.oblova_04 = sum(sBiotopa)
  ) %>%
  ungroup() %>%
  left_join(REKI, by = c("reka", "nazvReki"))

u_04 <- u_03 %>%
  group_by(reka, nazvReki, cod1, vid) %>%
  summarise(
    pieces_04 = sum(PIECES_03),
    gramm_04 = sum(GRAMM_03),
    poimali.raz_04 = sum(poimali.raz_03)
  ) %>%
  ungroup() %>%
  right_join(t_04, by = c("reka", "nazvReki")) %>%
  mutate(
    pNAm2_04 = round(pieces_04 / (S.oblova_04 * 1000000), 6),
    gNAm2_04 =  round(gramm_04 / (S.oblova_04 * 1000000), 6),
    procent_04 = round(poimali.raz_04 * 100 / protralili.raz_04, 1),
    PIECES_04 = round(pNAm2_04 * sReki * 1000000, 0), 
    GRAMM_04 = round(gNAm2_04 * sReki * 1000000, 0)
  ) %>%
  select(-pieces_04, -gramm_04, -protralili.raz_04, -sReki, -S.oblova_04)

# реки итого
u_04_itogo <- u_04 %>%
  group_by(reka, nazvReki) %>%
  summarise(
    PIECES_04 = sum(PIECES_04),
    GRAMM_04 = sum(GRAMM_04),
    pNAm2_04 = sum(pNAm2_04),
    gNAm2_04 = sum(gNAm2_04),
    kol_vo_vidov = n()
  ) %>%
  ungroup() %>%
  mutate(
    pNAm2_04 = round(pNAm2_04, 1),
    gNAm2_04 = round(gNAm2_04, 1)
  )

###
#
#        05 - ИТОГО
#
t_05 <- t_04 %>%
  group_by() %>%
  summarise(
    protralili.raz_05 = sum(protralili.raz_04),
    s.vsex.east = sum(sReki)
  ) %>%
  ungroup() %>%
  mutate(prizrac = "1")

u_05 <- u_04 %>%
  group_by(cod1, vid) %>%
  summarise(
    PIECES_05 = sum(PIECES_04),
    GRAMM_05 = sum(GRAMM_04),
    poimali.raz_05 = sum(poimali.raz_04)
  ) %>%
  ungroup() %>%
  mutate(prizrac = "1") %>%
  left_join(t_05, by = "prizrac") %>%
  select(-prizrac) %>%
  mutate(
    pNAm2_05 = PIECES_05 / (s.vsex.east * 1000000),
    gNAm2_05 =  GRAMM_05 / (s.vsex.east * 1000000),
    procent_05 = round(poimali.raz_05 * 100 / protralili.raz_05, 1)
  ) %>%
  select(-poimali.raz_05, -protralili.raz_05, -s.vsex.east)

# все станции итого
u_05_itogo <- u_05 %>%
  group_by() %>%
  summarise(
    PIECES_05 = sum(PIECES_05),
    GRAMM_05 = sum(GRAMM_05),
    pNAm2_05 = sum(pNAm2_05),
    gNAm2_05 = sum(gNAm2_05)
  ) %>%
  ungroup() %>%
  mutate(
    pNAm2_05 = round(pNAm2_05, 3),
    gNAm2_05 = round(gNAm2_05, 3)
  )

#_______________________________
#
#          кластерный анализ 60 биотопов
#
#_______________________________
library(reshape2)
klast.analiz <- u_03 %>%
  select(biotop, vid, gNAm2_03) %>%
  left_join(BIOTOP[ , c("biotop", "nazvReki")], by = "biotop") %>%
  dcast(biotop ~ vid, value.var = "gNAm2_03")

klast.analiz[is.na(klast.analiz)] <- 0
rownames(klast.analiz) <- klast.analiz$biotop
klast.analiz.pca <- prcomp(klast.analiz[-1], scale = FALSE) 
plot(klast.analiz.pca,
     type = "l",
     xaxt="n",
     yaxt="n",
     ann=FALSE) # каменная осыпь, кол-во кластеров

d <- dist(klast.analiz[-1], method = "canberra") 
H.fit <- hclust(d, method="ward.D2")
plot(H.fit, main = '', lwd = 2.5, cex=1, ylab = "Расстояние")
nn <- 7
rect.hclust(H.fit, k=nn, border="black")

rm(list=c("klast.analiz", "klast.analiz.pca","d", "H.fit", "nn", "klaster"))

