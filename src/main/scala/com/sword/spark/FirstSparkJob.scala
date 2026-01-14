package com.sword.spark
import org.apache.spark.sql.types.{StringType}
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object FirstSparkJob {
/*
  def requireEnv(name: String): String =
    sys.env.getOrElse(name,
      throw new IllegalArgumentException(s"Missing ENV variable: $name")
    )
*/
  val JDBC_URL = "jdbc:postgresql://host.docker.internal:5432/sword_db" /*requireEnv("JDBC_URL")*/
  val DB_USER = "sword" /*requireEnv("DB_USER")*/
  val DB_PASS = "sword" /*requireEnv("DB_PASSWORD")*/
  val JDBC_PROPS = Map(
    "user" -> DB_USER,
    "password" -> DB_PASS,
    "driver" -> "org.postgresql.Driver"
  )

  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" /*requireEnv("KAFKA_BOOTSTRAP_SERVERS")*/
  /*val KAFKA_USERNAME = requireEnv("KAFKA_USERNAME")*/
  /*val KAFKA_PASSWORD = requireEnv("KAFKA_PASSWORD")*/

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First_Spark_Job")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
      .getOrCreate()

    import spark.implicits._

    val pDate = args(0)
    val vDate6 = pDate.substring(2, 8)
    val dfRefrCvMap = readJdbc(spark, "RAW.REFR_CV_MAP")
    val dfMaFileCmmsFileAdd = readJdbc(spark, "RAW.MA_FILE_CMMS_FILE_ADD")
    val dfCmmsCmplnRptPrc = readJdbc(spark, "RAW.CMMS_CMPLN_RPT_PRC")

    dfRefrCvMap.createOrReplaceTempView("REFR_CV_MAP")
    dfMaFileCmmsFileAdd.createOrReplaceTempView("MA_FILE_CMMS_FILE_ADD")
    dfCmmsCmplnRptPrc.createOrReplaceTempView("CMMS_CMPLN_RPT_PRC")
    // Buoc so 1: Lay danh sach cac bao cao can xu ly tu bang mapping
    val dfAllRpt = dfRefrCvMap
      .filter(col("SRC_GRP") === "CMMS_RPT_MASRC_CODE_MAP")
      .select(
        col("SRC_INFO_1").as("SRC_TBL"),
        col("TRGT_GRP").as("RPT_TYPE"),
        col("TRGT_INFO_1").as("RPT_CODE"),
        col("TRGT_INFO_2").as("FILE_FLAG")
      )

    // Buoc so 2: Loop qua tung bao cao de xu ly
    dfAllRpt.collect().foreach { row =>
      val srcTable = row.getAs[String]("SRC_TBL")
      val rptCode = row.getAs[String]("RPT_CODE")
      val fileFlag = row.getAs[String]("FILE_FLAG")
      val rptType = row.getAs[String]("RPT_TYPE")

      val dfSrcTable = readJdbc(spark, s"RAW.$srcTable")

      dfSrcTable.createOrReplaceTempView("SRC_RPT_TABLE")

      val queryCmmsCmplnRptPrcGetNewLog = s"""
                                             |(
                                             |SELECT
                                             |  *
                                             |FROM RAW.CMMS_CMPLN_RPT_PRC
                                             |WHERE SRC_TYPE = 'IMPORT'
                                             |  AND RPT_CODE = '$rptCode'
                                             |  AND UPDATE_TIME >= current_timestamp - interval '30 minutes'
                                             |) t
      """.stripMargin

      // Buoc so 3: Kiem tra bao cao co file dinh kem
      if (fileFlag == "RPT_FILE_ADD") {
        // lay danh sach bao cao can kiem tra file dinh kem trong 3 ngay gan nhat
        val dfCheckRptFileAdd3Day = spark.sql(
          s"""
             |SELECT DISTINCT
             |  BUS_DMN,
             |  RPT_ID
             |FROM SRC_RPT_TABLE
             |WHERE MA_STATUS <> 'P'
             |  AND $vDate6
             |      - CAST(SUBSTR(MA_EFF_CODE, 1, 6) AS BIGINT) <= 3
             |EXCEPT
             |SELECT
             |  BUS_DMN,
             |  RPT_ID
             |FROM CMMS_CMPLN_RPT_PRC
             |WHERE SRC_TYPE = 'IMPORT'
             |  AND RPT_CODE = '$rptCode'
             |  AND UPDATE_TIME >= TO_DATE('$vDate6', 'yymmdd') - 3
          """.stripMargin
        )

        dfCheckRptFileAdd3Day.createOrReplaceTempView("CHECK_RPT_FILE_ADD_3DAY")
        // dem so luong file dinh kem yeu cau va so luong file dinh kem thuc te
        val dfRptDetailFileAdd3Day = spark.sql(
          s"""
             |SELECT
             |  BUS_DMN,
             |  RPT_ID,
             |  COUNT(
             |    CASE
             |      WHEN FILE_ADD_FLAG = 'Y' THEN FILE_ADD_FLAG
             |    END
             |  ) AS FILE_ED,
             |  COUNT(
             |    DISTINCT CASE
             |      WHEN FILE_ADD_CASE <> 'NONE'
             |       AND FILE_ADD_FLAG = 'Y'
             |      THEN FILE_ADD_CASE
             |    END
             |  ) AS FILE_AT
             |FROM (
             |  SELECT
             |    A.*,
             |    NVL(C.CASE_ID, 'NONE') AS FILE_ADD_CASE
             |  FROM SRC_RPT_TABLE A
             |  JOIN CHECK_RPT_FILE_ADD_3DAY B
             |    ON B.BUS_DMN = A.BUS_DMN
             |   AND B.RPT_ID  = A.RPT_ID
             |  LEFT JOIN MA_FILE_CMMS_FILE_ADD C
             |    ON C.MA_STATUS <> 'P'
             |   AND C.BUS_DMN   = A.BUS_DMN
             |   AND C.RPT_CODE  = '$rptCode'
             |   AND C.RPT_ID    = A.RPT_ID
             |   AND C.CASE_ID   = A.CASE_ID
             |) D
             |GROUP BY
             |  BUS_DMN,
             |  RPT_ID
          """.stripMargin
        )

        val dfRptDetailFileAddErr = dfRptDetailFileAdd3Day
          .filter(col("FILE_ED") =!= col("FILE_AT"))
          .select(
            lit("IMPORT").as("SRC_TYPE"),
            col("BUS_DMN"),
            lit(rptCode).as("RPT_CODE"),
            col("RPT_ID"),
            lit(null).cast("long").as("TOTAL_MSG_STATUS"),
            lit(null).cast("long").as("TOTAL_MSG_RPT"),
            lit("FAIL#FILE_ADD").as("PRC_STATUS"),
            lit("Số lượng File đính kèm thực tế không khớp với số lượng cần có trong Báo cáo").as("PRC_DETAIL"),
            current_timestamp().as("UPDATE_TIME")
          )
          .cache()

        dfRptDetailFileAddErr.count()

        val dfCmmsCmplnRptPrcMenFileAddErr = dfRptDetailFileAddErr
          .select(col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("PRC_STATUS"),
            col("PRC_DETAIL"),
            current_timestamp().as("UPDATE_TIME")
          )
        // Luu log loi vao bang CMMS_CMPLN_RPT_PRC
        writeJdbc( dfRptDetailFileAddErr, "RAW.CMMS_CMPLN_RPT_PRC", SaveMode.Append )

        writeJdbc( dfCmmsCmplnRptPrcMenFileAddErr, "RAW.CMMS_CMPLN_RPT_PRC_MEM", SaveMode.Append )
      }
      // Buoc so 4: Lay danh sach bao cao can push len kafka trong 3 ngay gan nhat
      val dfLogFail = readJdbc(spark, "RAW.CMMS_CMPLN_RPT_PRC")

      dfLogFail.createOrReplaceTempView("LOG_FAIL")

      val dfBookRpt3Day = spark.sql(
        s"""
           |SELECT
           |  A.BUS_DMN,
           |  A.RPT_ID,
           |  CASE
           |    WHEN B.PRC_STATUS IS NULL THEN '1'
           |    ELSE '0'
           |  END AS PUSH_FLAG
           |FROM (
           |  SELECT DISTINCT
           |    BUS_DMN,
           |    RPT_ID
           |  FROM SRC_RPT_TABLE
           |  WHERE MA_STATUS <> 'P'
           |    AND $vDate6
           |        - CAST(SUBSTR(MA_EFF_CODE, 1, 6) AS BIGINT) <= 3
           |) A
           |LEFT JOIN LOG_FAIL B
           |  ON A.RPT_ID = B.RPT_ID
           | AND B.SRC_TYPE = 'IMPORT'
           | AND B.RPT_CODE = '$rptCode'
           | AND B.UPDATE_TIME >= TO_DATE('$vDate6', 'yymmdd') - 3
         """.stripMargin
      )

      dfBookRpt3Day.createOrReplaceTempView("BOOK_RPT_3DAY")
      // Buoc 4.2: Lay du lieu chi tiet bao cao can push len kafka
      val dfBookRpt3DayDetail = spark.sql(
        """
          |SELECT
          |  A.*
          |FROM SRC_RPT_TABLE A
          |JOIN BOOK_RPT_3DAY B
          |  ON B.BUS_DMN   = A.BUS_DMN
          | AND B.RPT_ID    = A.RPT_ID
          | AND B.PUSH_FLAG = '1'
        """.stripMargin
      )

      dfBookRpt3DayDetail.createOrReplaceTempView("BOOK_RPT_3DAY_DETAIL")
      // Buoc so 4.3: Lay du lieu chi tiet file dinh kem neu co
      val dfFinalFileAdd: Option[DataFrame] =
        if (fileFlag == "RPT_FILE_ADD") {
          val dfAttachment = spark.sql(
            s"""
               |SELECT
               |  A.*
               |FROM MA_FILE_CMMS_FILE_ADD A
               |JOIN BOOK_RPT_3DAY_DETAIL B
               |  ON B.BUS_DMN        = A.BUS_DMN
               | AND B.RPT_ID         = A.RPT_ID
               | AND A.RPT_CODE       = '$rptCode'
               | AND B.CASE_ID        = A.CASE_ID
               | AND B.FILE_ADD_FLAG  = 'Y'
              """.stripMargin
          )

          val dfFieldUnpivotFileAdd = dfRefrCvMap
            .filter(
              col("SRC_GRP") === "CMMS_RPT_FIELD" &&
                col("SRC_INFO_3") === "MA_FILE_CMMS_FILE_ADD"
            )
            .select(col("SRC_INFO_2").as("FIELD_NAME"))

          val arrayFieldUnpivotFileAdd = dfFieldUnpivotFileAdd.collect()

          val dfUnpivotedFileAdd = createUnpivotedDF(
            spark,
            dfAttachment,
            arrayFieldUnpivotFileAdd.map(_.getString(0))
          )

          val dfUnpivotedFileAddRpt = dfUnpivotedFileAdd
            .select(
              lit("Y").as("FILE_ADD"),
              col("BUS_DMN"),
              lit(rptCode).as("RPT_CODE"),
              col("RPT_ID"),
              concat(col("MA_EFF_CODE"), lit("-"), col("MA_KEY_ID")).as("RECORD_ID"),
              col("FIELD_NAME"),
              col("FIELD_VALUE")
            )

          Some(dfUnpivotedFileAddRpt)
        } else {
          None
        }

      // Buoc so 4.4: Unpivot du lieu bao cao chinh
      val dfFieldUnpivotRpt = spark.sql(
        s"""
           |SELECT
           |  SRC_INFO_2 AS FIELD_NAME
           |FROM REFR_CV_MAP
           |WHERE SRC_GRP    = 'CMMS_RPT_FIELD'
           |  AND SRC_INFO_3 = '$srcTable'
           |
           |UNION
           |
           |SELECT
           |  SRC_INFO_2 AS FIELD_NAME
           |FROM REFR_CV_MAP
           |WHERE SRC_GRP    = 'CMMS_RPT_FIELD'
           |  AND SRC_INFO_1 = 'RPT_MAIN'
           |  AND SRC_INFO_3 = 'MA_FILE'
          """.stripMargin
      )

      val mappingFieldUnpivotRpt = dfFieldUnpivotRpt.collect()

      val dfUnpivotedRpt = createUnpivotedDF(
        spark,
        dfBookRpt3DayDetail,
        mappingFieldUnpivotRpt.map(_.getString(0))
      )

      val dfFinalRpt = dfUnpivotedRpt
        .select(
          lit("N").as("FILE_ADD"),
          col("BUS_DMN"),
          lit(rptCode).as("RPT_CODE"),
          col("RPT_ID"),
          concat(col("MA_EFF_CODE"), lit("-"), col("MA_KEY_ID")).as("RECORD_ID"),
          col("FIELD_NAME"),
          col("FIELD_VALUE")
        )

      val dfUnionfFinalRpt = dfFinalFileAdd.foldLeft(dfFinalRpt) { (acc, dfAdd) =>
        acc.unionByName(dfAdd)
      }

      dfUnionfFinalRpt.createOrReplaceTempView("UNION_FINAL_RPT")

      val dfReportDataFrame = spark.sql(
        s"""
           |SELECT
           |  A.*,
           |  CASE
           |    WHEN A.FILE_ADD = 'N'
           |     AND B.SRC_INFO_2 IS NOT NULL THEN 'MAIN'
           |    WHEN A.FILE_ADD = 'N'
           |     AND C.SRC_INFO_2 IS NOT NULL THEN 'DETAIL'
           |    WHEN A.FILE_ADD = 'Y' THEN 'FILE_ADD'
           |  END AS INFO_TYPE
           |FROM UNION_FINAL_RPT A
           |LEFT JOIN REFR_CV_MAP B
           |  ON B.SRC_GRP     = 'CMMS_RPT_FIELD'
           | AND B.TRGT_GRP    = 'Y'
           | AND B.SRC_INFO_1  = 'RPT_MAIN'
           | AND B.SRC_INFO_2  = A.FIELD_NAME
           |LEFT JOIN REFR_CV_MAP C
           |  ON C.SRC_GRP     = 'CMMS_RPT_FIELD'
           | AND C.TRGT_GRP    = 'Y'
           | AND C.SRC_INFO_1  = '$rptCode'
           | AND C.SRC_INFO_2  = A.FIELD_NAME
           |ORDER BY
           |  BUS_DMN,
           |  RPT_ID
          """.stripMargin
      )

      if (rptType == "TYPE_1") {
        val srcMainRptType1 = dfReportDataFrame
          .filter(col("INFO_TYPE") === "MAIN")
        // Gom item theo RECORD_ID + INFO_TYPE
        val dfMainItemType1 = srcMainRptType1
          .groupBy(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), col("INFO_TYPE"), col("RECORD_ID"))
          .agg(
            collect_list(
              struct(
                col("FIELD_NAME").as("fieldName"),
                col("FIELD_VALUE").as("fieldValue")
              )
            ).as("item")
          )

        // Tạo rptDetail array
        val dfMainDetailType1 = dfMainItemType1
          .groupBy(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"))
          .agg(
            collect_list(
              struct(
                col("INFO_TYPE").as("infoType"),
                col("RECORD_ID").as("recordID"),
                col("item")
              )
            ).as("rptDetail")
          )

        // Tạo JSON
        val dfMainType1 = dfMainDetailType1
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            to_json(
              struct(
                lit("APP_ONEID_CMMS_CMPLN_RPT").as("table"),
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
                struct(
                  col("BUS_DMN").as("busDmn"),
                  when(col("BUS_DMN") === "MBB", lit("3zKMfka22kni3KYh5zwC"))
                    .when(col("BUS_DMN") === "MBCAP", lit("BrdW1Gf9OrGPynlB38yw"))
                    .as("busDmnKey"),
                  col("RPT_CODE").as("rptCode"),
                  col("RPT_ID").as("rptID")
                ).as("rptMain"),
                col("rptDetail")
              )
            ).as("value")
          )

        val srcDtlRptType1 = dfReportDataFrame
          .filter(col("INFO_TYPE") === "DETAIL")

        val dfDtlItemType1 = srcDtlRptType1
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("RECORD_ID"),
            struct(
              col("FIELD_NAME").as("fieldName"),
              col("FIELD_VALUE").as("fieldValue")
            ).as("item")
          )

        val dfDtlDetailType1 = dfDtlItemType1
          .groupBy(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("RECORD_ID")
          )
          .agg(
            collect_list(col("item")).as("item")
          )
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("RECORD_ID"),
            struct(
              lit("DETAIL").as("infoType"),
              col("RECORD_ID").as("recordID"),
              col("item")
            ).as("rptDetail")
          )


        val dfDtlFinalType1 = dfDtlDetailType1
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            to_json(
              struct(
                lit("APP_ONEID_CMMS_CMPLN_RPT").as("table"),
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
                struct(
                  col("BUS_DMN").as("busDmn"),
                  when(col("BUS_DMN") === "MBB", lit("3zKMfka22kni3KYh5zwC"))
                    .when(col("BUS_DMN") === "MBCAP", lit("BrdW1Gf9OrGPynlB38yw"))
                    .as("busDmnKey"),
                  col("RPT_CODE").as("rptCode"),
                  col("RPT_ID").as("rptID")
                ).as("rptMain"),
                array($"rptDetail").as("rptDetail")
              )
            ).as("value")
          )

        val dfFileAddType1 = dfReportDataFrame
          .filter(col("INFO_TYPE") === "FILE_ADD")


        val dfFileAddItemType1 = dfFileAddType1
          .filter($"FILE_ADD" === "Y")
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("RECORD_ID"),
            struct(
              col("FIELD_NAME").as("fieldName"),
              col("FIELD_VALUE").cast("string").as("fieldValue")
            ).as("item")
          )

        val dfFileAddDetailType1 = dfFileAddItemType1
          .groupBy(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("RECORD_ID")
          )
          .agg(
            collect_list(col("item")).as("item")
          )
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            struct(
              lit("FILE_ADD").as("infoType"),
              col("RECORD_ID").as("recordID"),
              lit(null).cast("string").as("itemGroup"),
              col("item")
            ).as("rptDetail")
          )

        val dfFileAddFinalType1 = dfFileAddDetailType1
          .groupBy(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID")
          )
          .agg(
            collect_list(col("rptDetail")).as("rptDetail")
          )
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            to_json(
              struct(
                lit("APP_ONEID_CMMS_CMPLN_RPT").as("table"),
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
                struct(
                  col("BUS_DMN").as("busDmn"),
                  when(col("BUS_DMN") === "MBB", lit("3zKMfka22kni3KYh5zwC"))
                    .when(col("BUS_DMN") === "MBCAP", lit("BrdW1Gf9OrGPynlB38yw"))
                    .as("busDmnKey"),
                  col("RPT_CODE").as("rptCode"),
                  col("RPT_ID").as("rptID")
                ).as("rptMain"),
                col("rptDetail")
              )
            ).as("value")
          )

        val dfJsonUnionRptType1 = dfMainType1
          .select(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), col("value"))
          .union(dfDtlFinalType1.select(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), col("value")))
          .union(dfFileAddFinalType1.select(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), col("value")))
          .distinct()

        val dfJsonRptType1 = dfJsonUnionRptType1.select(col("value"))

        // writeKafka( dfJsonRptType1, "APP_ONEID_CMMS_CMPLN_RPT" )

        val dfLogRptType1 = dfJsonUnionRptType1
          .groupBy(col("BUS_DMN"), col("RPT_ID"))
          .agg(
            count("*").as("TOTAL_MSG_STATUS"),
            lit(null).cast("long").as("TOTAL_MSG_RPT")
          )
          .select(
            lit("IMPORT").as("SRC_TYPE"),
            col("BUS_DMN"),
            lit(rptCode).as("RPT_CODE"),
            col("RPT_ID"),
            col("TOTAL_MSG_STATUS"),
            col("TOTAL_MSG_RPT"),
            lit("SUCCESS#PUSH_KAFKA").as("PRC_STATUS"),
            lit("Đẩy báo cáo lên Kafka thành công").as("PRC_DETAIL"),
            current_timestamp().as("UPDATE_TIME")
          )

        val dfKeyLogRptType1 = dfLogRptType1
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID")
          ).distinct()
          .cache()

        dfKeyLogRptType1.count()

        writeJdbc( dfLogRptType1, "RAW.CMMS_CMPLN_RPT_PRC", SaveMode.Append )

        val dfCmmsCmplnRptPrcLogType1 = readJdbc(spark, queryCmmsCmplnRptPrcGetNewLog)

        val dfInCmmsCmplnRptPrcLogType1 = dfCmmsCmplnRptPrcLogType1
          .join(
            dfKeyLogRptType1,
            Seq("BUS_DMN", "RPT_CODE", "RPT_ID"),
            "inner"
          )

        writeJdbc( dfInCmmsCmplnRptPrcLogType1, "RAW.dfInCmmsCmplnRptPrcLogType1", SaveMode.Append )

        val dfCmmsCmplnRptPrcType1 = dfInCmmsCmplnRptPrcLogType1
          .filter(col("PRC_STATUS") === "SUCCESS#PUSH_KAFKA")
          .select(
            to_json(
              struct(
                lit("APP_ONEID_CMMS_CMPLN_RPT_JOB_STATUS").as("table"),
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
                struct(
                  col("SRC_TYPE").as("rptMain"),
                  col("BUS_DMN").as("busDmn"),
                  when(col("BUS_DMN") === "MBB", lit("3zKMfka22kni3KYh5zwC"))
                    .when(col("BUS_DMN") === "MBCAP", lit("BrdW1Gf9OrGPynlB38yw"))
                    .as("busDmnKey"),
                  col("RPT_CODE").as("rptCode"),
                  col("RPT_ID").as("rptID")
                ).as("rptMain"),
                struct(
                  lit("CMMS_CMPLN_RPT_IMPORT").as("jobName"),
                  lit("APP_ONEID_CMMS_CMPLN_RPT").as("targetTable"),
                  lit("SUCCESS").as("status"),
                  date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("startTime"),
                  date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("endTime"),
                  col("TOTAL_MSG_STATUS").as("srcRecordCount"),
                  col("TOTAL_MSG_STATUS").as("targetRecordCount")
                ).as("jobStatus")
              )
            ).as("value")
          )

        // writeKafka( dfCmmsCmplnRptPrcType1, "APP_ONEID_CMMS_CMPLN_RPT_JOB_STATUS" )

        val dfCmmsCmplnRptPrcMenType1 = dfInCmmsCmplnRptPrcLogType1
          .select(col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("PRC_STATUS"),
            col("PRC_DETAIL"),
            current_timestamp().as("UPDATE_TIME")
          )

        writeJdbc( dfCmmsCmplnRptPrcMenType1, "RAW.CMMS_CMPLN_RPT_PRC_MEM", SaveMode.Append )
      }
      else if (rptType == "TYPE_2") {

        // rptMain
        val dfRptMainType2 = dfReportDataFrame
          .select(
            col("BUS_DMN").as("busDmn"),
            col("RPT_CODE").as("rptCode"),
            col("RPT_ID").as("rptID"),
            when(col("BUS_DMN") === "MBB", lit("3zKMfka22kni3KYh5zwC"))
              .when(col("BUS_DMN") === "MBCAP", lit("BrdW1Gf9OrGPynlB38yw"))
              .otherwise(lit(null))
              .as("busDmnKey")
          )
          .distinct()


        // rptDetail - MAIN
        val dfDtlMainType2 = dfReportDataFrame
          .filter(col("INFO_TYPE") === "MAIN")
          .groupBy(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), col("RECORD_ID"))
          .agg(
            sort_array(
              collect_list(
                struct(
                  col("FIELD_NAME").as("fieldName"),
                  col("FIELD_VALUE").as("fieldValue")
                )
              )
            ).as("item")
          )
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            struct(
              lit("MAIN").as("infoType"),
              col("RECORD_ID").as("recordID"),
              lit(null).cast(StringType).as("itemGroup"),
              col("item")
            ).as("rptDetail")
          )

        // rptDetailDetail
        val dfDetlDtlType2 =
          dfReportDataFrame
            .filter(col("INFO_TYPE") === "DETAIL")
            .groupBy(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), col("RECORD_ID"))
            .agg(
              sort_array(
                collect_list(
                  struct(
                    col("FIELD_NAME").as("fieldName"),
                    col("FIELD_VALUE").as("fieldValue")
                  )
                )
              ).as("item")
            )
            .select(
              col("BUS_DMN"),
              col("RPT_CODE"),
              col("RPT_ID"),
              struct(
                lit("DETAIL").as("infoType"),
                col("RECORD_ID").as("recordID"),
                lit(null).cast(StringType).as("itemGroup"),
                col("item")
              ).as("rptDetail")
            )

        val dfRptDtlType2 = dfDtlMainType2
          .unionByName(dfDetlDtlType2)

        val dfRptDtlAggType2 =
          dfRptDtlType2
            .withColumn(
              "sortKey",
              when(col("rptDetail.infoType") === "MAIN", 0).otherwise(1)
            )
            .orderBy(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"), $"sortKey")
            .groupBy(col("BUS_DMN"), col("RPT_CODE"), col("RPT_ID"))
            .agg(
              collect_list(col("rptDetail")).as("rptDetail")
            )

        val dfRptFinalType2 =
          dfRptMainType2
            .join(
              dfRptDtlAggType2,
              dfRptMainType2("busDmn") === dfRptDtlAggType2("BUS_DMN") &&
                dfRptMainType2("rptCode") === dfRptDtlAggType2("RPT_CODE") &&
                dfRptMainType2("rptID") === dfRptDtlAggType2("RPT_ID")
            )
            .select(
              lit("APP_ONEID_CMMS_CMPLN_RPT").as("table"),
              date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
              struct(col("busDmn"), col("busDmnKey"), col("rptCode"), col("rptID")).as("rptMain"),
              col("rptDetail")
            )

        val dfJsonRptType2 = dfRptFinalType2
          .select(
            to_json(
              struct(
                lit("APP_ONEID_CMMS_CMPLN_RPT").as("table"),
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
                col("rptMain"),
                col("rptDetail")
              )
            ).as("value")
          )

        // writeKafka( dfJsonRptType2, "APP_ONEID_CMMS_CMPLN_RPT" )

        val dfLogRptType2 = dfRptMainType2
          .groupBy(
            col("busDmn"),
            col("rptID")
          )
          .agg(
            count("*").as("TOTAL_MSG_STATUS")
          )
          .withColumnRenamed("busDmn", "BUS_DMN")
          .withColumnRenamed("rptID", "RPT_ID")
          .withColumn("RPT_CODE", lit(rptCode))
          .withColumn("SRC_TYPE", lit("IMPORT"))
          .withColumn("TOTAL_MSG_RPT", lit(null).cast("bigint"))
          .withColumn("PRC_STATUS", lit("SUCCESS#PUSH_KAFKA"))
          .withColumn("PRC_DETAIL", lit("Đẩy báo cáo lên Kafka thành công"))
          .withColumn("UPDATE_TIME", current_timestamp())

        val dfKeyLogRptType2 = dfLogRptType2
          .select(
            col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID")
          ).distinct()
          .cache()

        dfKeyLogRptType2.count()

        writeJdbc( dfLogRptType2, "RAW.CMMS_CMPLN_RPT_PRC", SaveMode.Append )

        val dfCmmsCmplnRptPrcLogType2 = readJdbc(spark, queryCmmsCmplnRptPrcGetNewLog)

        val dfInCmmsCmplnRptPrcLogType2 = dfCmmsCmplnRptPrcLogType2
          .join(
            dfKeyLogRptType2,
            Seq("BUS_DMN", "RPT_CODE", "RPT_ID"),
            "inner"
          )

        val dfCmmsCmplnRptPrcType2 = dfInCmmsCmplnRptPrcLogType2
          .filter(col("PRC_STATUS") === "SUCCESS#PUSH_KAFKA")
          .select(
            to_json(
              struct(
                lit("APP_ONEID_CMMS_CMPLN_RPT_JOB_STATUS").as("table"),
                date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("op_ts"),
                struct(
                  col("SRC_TYPE").as("rptMain"),
                  col("BUS_DMN").as("busDmn"),
                  when(col("BUS_DMN") === "MBB", lit("3zKMfka22kni3KYh5zwC"))
                    .when(col("BUS_DMN") === "MBCAP", lit("BrdW1Gf9OrGPynlB38yw"))
                    .as("busDmnKey"),
                  col("RPT_CODE").as("rptCode"),
                  col("RPT_ID").as("rptID")
                ).as("rptMain"),
                struct(
                  lit("CMMS_CMPLN_RPT_IMPORT").as("jobName"),
                  lit("APP_ONEID_CMMS_CMPLN_RPT").as("targetTable"),
                  lit("SUCCESS").as("status"),
                  date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("startTime"),
                  date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS").as("endTime"),
                  col("TOTAL_MSG_STATUS").as("srcRecordCount"),
                  col("TOTAL_MSG_STATUS").as("targetRecordCount")
                ).as("jobStatus")
              )
            ).as("value")
          )

        // writeKafka( dfCmmsCmplnRptPrcType2, "APP_ONEID_CMMS_CMPLN_RPT_JOB_STATUS" )

        val dfCmmsCmplnRptPrcMenType2 = dfInCmmsCmplnRptPrcLogType2
          .select(col("BUS_DMN"),
            col("RPT_CODE"),
            col("RPT_ID"),
            col("PRC_STATUS"),
            col("PRC_DETAIL"),
            current_timestamp().as("UPDATE_TIME")
          )

        writeJdbc( dfCmmsCmplnRptPrcMenType2, "RAW.CMMS_CMPLN_RPT_PRC_MEM", SaveMode.Append )
      }
    }
    spark.stop()
  }

  def createUnpivotedDF(
                         spark: SparkSession,
                         df: DataFrame,
                         fieldNames: Array[String]
                       ): DataFrame = {
    if (fieldNames.isEmpty) {
      df.select(
        df.columns.map(col) :+
          lit(null).cast("string").as("field_name") :+
          lit(null).cast("string").as("field_value"): _*
      )
    } else {
      val listFieldFixed = df.columns.filterNot(colName =>
        fieldNames.map(_.toLowerCase).contains(colName.toLowerCase)
      ).map(col)

      val stackExpr = fieldNames.map { fieldName =>
        s"'$fieldName', CAST(`${fieldName.toLowerCase}` AS STRING)"
      }.mkString(", ")

      df.select(
        listFieldFixed :+ expr(s"stack(${fieldNames.length}, $stackExpr) as (field_name, field_value)"): _*
      )
    }
  }

  def readJdbc(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", JDBC_URL)
      .options(JDBC_PROPS)
      .option("dbtable", tableName)
      .load()
  }

  def writeJdbc(df: DataFrame, tableName: String, mode: SaveMode): Unit = {
    df.write
      .format("jdbc")
      .option("url", JDBC_URL)
      .options(JDBC_PROPS)
      .option("dbtable", tableName)
      .mode(mode)
      .save()
  }

  // def writeKafka(df: DataFrame, topic: String): Unit = {
  //   df.write
  //     .format("kafka")
  //     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
  //     .option("topic", topic)
  //     .save()
  // }
}