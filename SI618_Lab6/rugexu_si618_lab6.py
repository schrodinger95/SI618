import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import *

lol = sqlContext.read.csv("hdfs:///var/umsi618f21/lab6/na_ranked_team.csv", header=True)
lol.registerTempTable("lol")

KDA = sqlContext.sql("SELECT *, (cast(kill as int) + cast(assist as int)) / (cast(death as int) + 1) as KDA from lol")
KDA.registerTempTable("lol_KDA")
q1 = sqlContext.sql("SELECT summonerId, count(*) as matches, AVG(KDA) as avgKDA from lol_KDA GROUP BY summonerId HAVING count(*)>= 10 ORDER BY avgKDA DESC")
q1.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('rugexu_si618_lab6_output_1')

avgKDA = sqlContext.sql("SELECT matchID, winner, AVG(KDA) as avgKDA from lol_KDA GROUP BY matchID, winner")
avgKDA.registerTempTable("lol_avgKDA")
lol_KDA_avgKDA = sqlContext.sql("SELECT lol_KDA.summonerId, lol_KDA.KDA, lol_avgKDA.avgKDA from lol_KDA INNER JOIN lol_avgKDA ON (lol_KDA.matchID=lol_avgKDA.matchID) AND (lol_KDA.winner=lol_avgKDA.winner)")
lol_KDA_avgKDA.registerTempTable("lol_KDA_avgKDA")
q2 = sqlContext.sql("SELECT summonerId, count(*) as matches, AVG(KDA / (avgKDA + 1)) as normKDA from lol_KDA_avgKDA GROUP BY summonerId HAVING count(*)>= 10 ORDER BY normKDA DESC")
q2.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('rugexu_si618_lab6_output_2')

win_lol = sqlContext.sql("SELECT matchId, predictedRole, championName as Champion1, KDA as KDA1 from lol_KDA WHERE winner=1")
win_lol.registerTempTable("win_lol")
lose_lol = sqlContext.sql("SELECT matchId, predictedRole, championName as Champion2, KDA as KDA2 from lol_KDA WHERE winner=0")
lose_lol.registerTempTable("lose_lol")
win_lose_lol = sqlContext.sql("SELECT win_lol.matchID, win_lol.predictedRole, win_lol.Champion1, win_lol.KDA1, lose_lol.Champion2, lose_lol.KDA2 from win_lol INNER JOIN lose_lol ON (win_lol.matchId=lose_lol.matchId) AND (win_lol.predictedRole=lose_lol.predictedRole)")
win_lose_lol.registerTempTable("win_lose_lol")
win_lose_lol1 = sqlContext.sql("SELECT * from win_lose_lol WHERE Champion1 < Champion2")
win_lose_lol1.registerTempTable("win_lose_lol1")
win_lose_lol2 = sqlContext.sql("SELECT matchId, predictedRole, Champion2 as Champion1, KDA2 as KDA1, Champion1 as Champion2, KDA1 as KDA2 from win_lose_lol WHERE Champion1 > Champion2")
win_lose_lol2.registerTempTable("win_lose_lol2")
sorted_win_lose_lol = sqlContext.sql("SELECT * from win_lose_lol1 UNION ALL SELECT * from win_lose_lol2")
sorted_win_lose_lol.registerTempTable("sorted_win_lose_lol")
q3 = sqlContext.sql("SELECT Champion1, Champion2, predictedRole, count(*) as matches, AVG(KDA1 / KDA2) as avgKDA from sorted_win_lose_lol GROUP BY Champion1, Champion2, predictedRole HAVING count(*)>= 10 ORDER BY Champion1, predictedRole, matches DESC, Champion2")
q3.rdd.map(lambda i: '\t'.join(str(j) for j in i)).saveAsTextFile('rugexu_si618_lab6_output_3')