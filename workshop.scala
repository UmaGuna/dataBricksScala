// Load Two csv files
// upload the csv file in data bricks

%scala
val countryDF = spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema","true")
                .load("/FileStore/tables.Country.csv")

display(countryDF)    
countryDF.printSchema() 

val indicatorDF = spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema","true")
                .load("/FileStore/tables.Indicators.csv")

display(indicatorDF)  
indicatorDF.printSchema()

// create a temparary view
countryDF.createOrReplaceTempView("CountryTmpTbl")
spark.sql("select * from CountryTmpTable").show()

indicatorDF.createOrReplaceTempView("IndicatorTmpTbl")
spark.sql("select * from IndicatorTmpTable").show()

//run sql commands
%sql
select * from CountryTmpTable limit 5

//findout Gini index for india
select year,value
from IndicatorTmpTable
where indicatorCode == "XX.GINI"  and countryCode =="IND"
order by year desc


//Dataframe for above sql
%scala
val giniIndex = indicatorDF.select(year,value)
                .filter($"indicatorCode" === "XX.GINI" and "countryCode" ==="IND")
                .orderBy("year" desc)
giniIndex.show()


// youth literacy rate for 1990
%sql
select value as Youth_Literacy_rate , ShortName
from CountryTmpTable c, IndicatorTmpTable i
on c.countryCode = i.countryCode
where indicatorCode = "YT.LT.zs" and year=1990
order by Youth_Literacy_rate desc

%scala
val joinDF = indicatorDF.join(countryDF, indicatorDF("countryCode") === countryDF("countryCode"))
val filtDF = joinDF.select(value , ShortName).filter($"indicatorCode" === "YT.LT.zs"  and $"year" === "1990")   
val sortDF  = filtDF.withColumnRenamed("value", "Youth_Literacy_rate").orderBy($"Youth_Literacy_rate" desc)

sortDF.show()


//Trade percentage between india & china
%sql
select year,value
from IndicatorTmpTable 
where indicatorCode = "TRD.ZS" and countryCode in ("IND","CHN")
order by year desc

%scala
val traDF = indicatorDF.select(year,value)
                .filter($"indicatorCode" === "TRD.ZS" and $"countryCode".isin("IND","CHN")) 
                .orderBy($"year" desc)

traDF.show()                
