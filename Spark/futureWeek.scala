package com.haiteam


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

import scala.collection.mutable

object futureWeek {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /////////////////////////  빈주차 생성시 사용되는 함수 ////////////////////////////////////

    def getLastWeek(year: Int): Int = { // Lastweek 가 언제인지 알려주는 함수

      val calendar: java.util.Calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      val dateFormat: java.text.SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      calendar.setTime(dateFormat.parse(year + "1231"))

      return calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
    }

    def postWeek(inputYearWeek: Any, gapWeek: Any): String = { //
      var strInputYearWeek: String = inputYearWeek.toString() // substring 해주기 위해 String 으로 형 변환
    var strGapWeek: String = gapWeek.toString() // substring 해주기 위해 String 으로 형 변환

      var inputYear: Int = Integer.parseInt(strInputYearWeek.substring(0, 4)) // 2015  다시 int 형으로
      var inputWeek: Int = Integer.parseInt(strInputYearWeek.substring(4)) //  17
      var inputGapWeek: Int = Integer.parseInt(strGapWeek) // string 형인 strGapWeek 도 int 형으로

      var calcWeek: Int = inputWeek + inputGapWeek.abs // 17 + 0 (1씩 증가함) abs 는 왜..?

      while (calcWeek > getLastWeek(inputYear)) { // 17 > 52  ( 2015년의 라스트주차 예를들어 52 )
        calcWeek -= getLastWeek(inputYear)
        inputYear += 1
      }

      var resultYear: String = inputYear.toString() // 2015 가 resultYear 에 담김
      var resultWeek: String = calcWeek.toString() // 17 resultWeek 에 담김

      if (resultWeek.length < 2) {
        resultWeek = "0" + resultWeek
      }
      return resultYear + resultWeek // 2015 + 17
    }

    def preWeek(inputYearWeek: Any, gapWeek: Any): String = {
      var strInputYearWeek: String = inputYearWeek.toString
      var strGapWeek: String = gapWeek.toString

      var inputYear: Int = Integer.parseInt(strInputYearWeek.substring(0, 4))
      var inputWeek: Int = Integer.parseInt(strInputYearWeek.substring(4))
      var inputGapWeek: Int = Integer.parseInt(strGapWeek)

      var calcWeek: Int = inputWeek - inputGapWeek.abs

      while (calcWeek <= 0) {
        inputYear -= 1
        calcWeek += getLastWeek(inputYear)
      }

      var resultYear: String = inputYear.toString
      var resultWeek: String = calcWeek.toString
      if (resultWeek.length < 2) {
        resultWeek = "0" + resultWeek
      }
      return resultYear + resultWeek
    }

    /////////////////////////  빈주차 생성시 사용되는 함수 끝~~ ////////////////////////////////////


    var targetFile = "pro_actual_sales.csv"

    // 절대경로 입력
    var salesData =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + targetFile)

    val salesColumns = salesData.columns
    val noRegionSeg1 = salesColumns.indexOf("regionSeg1") // ex 미국
    val noProductSeg1 = salesColumns.indexOf("productSeg1") // 널값
    val noProductSeg2 = salesColumns.indexOf("productSeg2") // 냉장고
    val noRegionSeg2 = salesColumns.indexOf("regionSeg2") // 이마트
    val noRegionSeg3 = salesColumns.indexOf("regionSeg3") // 플로리다
    val noProductSeg3 = salesColumns.indexOf("productSeg3") // 냉장고001 ( 냉장고 제품번호 )
    val noYearweek = salesColumns.indexOf("yearweek")
    val noYear = salesColumns.indexOf("year")
    val noWeek = salesColumns.indexOf("week")
    val noQty = salesColumns.indexOf("qty")

    val selloutRdd = salesData.rdd

    val maxYearweek1 = salesData.select("yearweek").collect.map(x => {
      x.getString(0)
    }).max // 최대 연주차 정보  // 201627

    val maxYearweek2 = selloutRdd.map(x => {
      x.getString(noYearweek)
    }).max // 최대 연주차 정보  // 201627

    var weeksAgo = preWeek(maxYearweek2, 4) // maxyear 에서 4주전, 201623

    val resultRdd = selloutRdd.groupBy(x => { // 그룹바이
          (
            x.getString(noRegionSeg1), // 미국
            x.getString(noProductSeg1), // 널값
            x.getString(noProductSeg2), // 냉장고
            x.getString(noRegionSeg2), // 이마트
            x.getString(noRegionSeg3), // 플로리다
            x.getString(noProductSeg3) // 냉장고001
          )
        }).flatMap(f = x => {
          val key = x._1 // 위 그룹바이 한 애들이 키값으로 들어가 있음
          val data = x._2 // 디버깅 할때 data.head 로 한줄 확인 가능

          // Using Difference Set
          val yearweekArray = data.map(x => { // data 중에서 noYearweek 해당하는 애들만 뽑아서 배열에 넣겟다!
            x.getString(noYearweek)
          }).toArray

          var minYearweek: String = yearweekArray.min // 201517  , min 값 구함
          var maxYearweek: String = yearweekArray.max // 201627  , max 값 구함

          var fullYearweek = Array.empty[String] // 빈배열을 만듬
          var currentYearweek = "" // 빈 스트링 만듬
          var gapWeek = 0 // gapWeek 는 0

          do {
            currentYearweek = postWeek(minYearweek, gapWeek) // 그룹단위 minyearweek, 갭주차 0
            fullYearweek ++= Array(currentYearweek) // 위 postWeek 의 리턴값을 currentYearweek 에 담고 위에서 만든 빈 배열에 더해줌
            gapWeek += 1 // gapWeek 를 1씩 증가시킴
          } while (currentYearweek != maxYearweek) // 해당 조건식이 true 면 시작 false 면 끝
          val emptyYearweek: Array[String] = fullYearweek.diff(yearweekArray) // 201519 , 201624

          val emptyMap = emptyYearweek.map(yearweek => { // 비어있는 emptyYearweek 을 map 연산을 함
            val year = yearweek.substring(0, 4)
            var week = yearweek.substring(4)
            var weekInt = week.toInt  // ex) 바로 위 week 에서 09가 들어왓으면, 인트로 바꿔주면 0 탈락후 9만 남게됨
            if (weekInt<10) {  // 9<10 이므로, true, 아래로 내려가서
              week = weekInt.toString   // 9인 weekInt 를 다시 스트링으로 변환 후, week 담아 마무리
            }else {                     // 그럼 기존데이터처럼 09가 아닌 1자리수 주차로 처리됨
              week
            }
        val qty = 0.0d // 비어 있는 emptyYearweek 에 대해서 qty 값이 없기 때문에 0으로 채워줌
        Row( // Row 를 사용하면 아래 data 와 emptyMap 더해줄때 타입을 emptyMap.toIterable 식으로 바꿔줘야함
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨. -> 맞나 체크
          yearweek, // 비어있는 yearweek 가 들어옴
          year, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          week, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          qty.toString, // 위에서 0으로 채워준거 적어줌
          "0"
          //output : (noProductSeg1, noProductSeg2, noRegionSeg2, noRegionSeg3, noProductSeg3, noProductSeg3
        )
      })
      ////////////////////////////////////  미래주차 생성 ////////////////////////////////////

      //      var fullYearweek2 = Array.empty[String] // 빈배열을 만듬
      //      var currentYearweek2 = "" // 빈 스트링 만듬
      //      var gapWeek2 = 1 // gapWeek 는 0
      //      var postWeek_input = 4 // 최대 주차 + 4주차까지 구하겟다!
      //      var futureYearweek = postWeek(maxYearweek, postWeek_input) // 함수를 이용해서!
      //
      //      do {
      //        currentYearweek2 = postWeek(maxYearweek, gapWeek2) // 그룹단위 maxYearweek
      //        fullYearweek2 ++= Array(currentYearweek2) // 위 postWeek 의 리턴값을 currentYearweek 에 담고 위에서 만든 빈 배열에 더해줌
      //        gapWeek2 += 1 // gapWeek 를 1씩 증가시킴
      //      } while (currentYearweek2 != futureYearweek) // 해당 조건식이 true 면 시작 false 면 끝
      //      val emptyYearweek2: Array[String] = fullYearweek2 // 201519 , 201624
      //
      //      val fcstRow = emptyYearweek2.map(yearweek => { // 비어있는 emptyYearweek 을 map 연산을 함
      //        val fcstyear = yearweek.substring(0, 4)
      //        val fcstweek = yearweek.substring(4)
      //        val fcstqty = 0.0d // 비어 있는 emptyYearweek 에 대해서 qty 값이 없기 때문에 0으로 채워줌
      //        var fcst = 0.0d  // fcst 컬럼 추가
      //        Row( // Row 를 사용하면 아래 data 와 emptyMap 더해줄때 타입을 emptyMap.toIterable 식으로 바꿔줘야함
      //          key._1,
      //          key._2,
      //          key._3,
      //          key._4,
      //          key._5,
      //          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨. -> 맞나 체크
      //          yearweek, // 비어있는 yearweek 가 들어옴
      //          fcstyear, // 비어있는 yearweek 를 바로 위에서 substring 한 값
      //          fcstweek, // 비어있는 yearweek 를 바로 위에서 substring 한 값
      //          fcstqty.toString, // 위에서 0으로 채워준거 적어줌
      //          fcst.toString
      //          //output : (noProductSeg1, noProductSeg2, noRegionSeg2, noRegionSeg3, noProductSeg3, noProductSeg3
      //        )
      //      })

      /////////////////////////////////////  미래주차 생성 ////////////////////////////////////
      val originMap = data.map(x => {
        Row(
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6,
          x.getString(noYearweek),
          x.getString(noYear),
          x.getString(noWeek),
          x.get(noQty).toString.toDouble,
          "0"
        )
      })

      originMap ++ emptyMap.toIterable // 모든 data 에 emptyMap (비어있는 yearweek 에 대해 설정한 놈) 을 더해줌
    }).filter(x => { //
      (x.getString(noWeek).toInt <= 52) // row 형태면 getString 으로 가져오고, row 안쓰면 시리즈였나 그 상태면 x._7 으로 가져와야함
    })


    var step10 = resultRdd.filter(x=>{   //  기존 data 와 빈주차생성한 데이터의 합친 rdd (resultRdd) 에 대해
      var checkValid = false           // 아래 조건으로 검색, 이유는...
      if(                               // 10주차, 미만 주차가 2개가 생성이 되는데, 추측하기로
        (x.getString(noWeek).toInt <= 10) &&   // 기존 데이타는 10주차 미만일때, 1,2,3,4,5... 9주로 나오는데(디버깅해봄)
          (x.getString(noRegionSeg1) == "A01") &&  // 빈주차생성한 놈들중에 10주차 미만인애들은 01,02,03...09 주차로 나옴
        (x.getString(noProductSeg2) == "PG01") &&   // 그게 resultRdd 즉, 합쳐질때 1주차와, 01주차 로 나뉘에 지기 때문에
        (x.getString(noRegionSeg2) == "SALESID0001") &&  // 2개씩 나오는 문제가 발생하는거로 예상됨,
        (x.getString(noRegionSeg3) == "SITEID0007") &&   // 그걸 체크하기 위한 디버깅코드임.
          (x.getString(noYearweek) == "201509")
      //          (x._1._5 == "5")     //
      ){
        checkValid = true
      }else{
        checkValid = false
      }
      checkValid
    })

    // resultRdd.take(68).foreach(println)

    var resultRdd2 = resultRdd.filter(x => { //
      //      (x.getString(noWeek).toInt <= 52) && // row 형태면 getString 으로 가져오고, row 안쓰면 시리즈였나 그 상태면 x._7 으로 가져와야함
      (x.getString(noYearweek).toInt > weeksAgo.toString.toDouble) && // weeksAgo 보다 큰 값만가져옴
        (x.getString(noYearweek).toInt <= maxYearweek2.toString.toDouble)
    }) // 24 25 26 27 4개값 뽑아옴

    // resultRdd2.take(20).foreach(println)

    var finalRdd = resultRdd2.groupBy(x => { //
      (
        x.getString(noRegionSeg1), // 미국
        x.getString(noProductSeg1), // 널값
        x.getString(noProductSeg2), // 냉장고
        x.getString(noRegionSeg2), // 이마트
        x.getString(noRegionSeg3), // 플로리다
        x.getString(noProductSeg3) // 냉장고001
      )
    }).flatMap(x => {
      var key = x._1
      var data = x._2
      var size = data.size

      var qtyList = data.map(x => { // QTY 에 해당하는놈만 qtyList 담는다
        x.get(noQty).toString.toDouble
      })
      // var qtyList = data.map(x=>{x.get(noQty).toString.toDouble})
      // 처음에 어떤 데이터타입이 들어올지 모르기 때문에, get 을 사용후, 원하는 데이터 산출을 위한 타입 캐스팅
      // 이렇게 하면 편하긴 하지만, 속도가 느리다는 단점이 있다.
      var qtyArray = qtyList.toArray
      var qtySum = qtyArray.sum // QTY 총합을 구함
      var qtyMean = if (size != 0) {
        qtySum / size // QTY 평균을 구함
      } else {
        0.0 // size가 0이면 0으로 해준다, 예외처리
      }

      ////////////////////////////////////  미래주차 생성 ////////////////////////////////////

      var fullYearweek2 = Array.empty[String] // 빈배열을 만듬
      var currentYearweek2 = "" // 빈 스트링 만듬
      var gapWeek2 = 1 // gapWeek 는 0
      var postWeek_input = 4 // 최대 주차 + 4주차까지 구하겟다!
      var futureYearweek = postWeek(maxYearweek2, postWeek_input) // 함수를 이용해서!

      do {
        currentYearweek2 = postWeek(maxYearweek2, gapWeek2) // 그룹단위 maxYearweek
        fullYearweek2 ++= Array(currentYearweek2) // 위 postWeek 의 리턴값을 currentYearweek 에 담고 위에서 만든 빈 배열에 더해줌
        gapWeek2 += 1 // gapWeek 를 1씩 증가시킴
      } while (currentYearweek2 != futureYearweek) // 해당 조건식이 true 면 시작 false 면 끝
      val emptyYearweek2: Array[String] = fullYearweek2 // 201519 , 201624

      val fcstRow = emptyYearweek2.map(yearweek => { // 비어있는 emptyYearweek 을 map 연산을 함
        val fcstyear = yearweek.substring(0, 4)
        var fcstweek = yearweek.substring(4)
        var fcstweekInt = fcstweek.toInt
        if (fcstweekInt<10) {
          fcstweek = fcstweekInt.toString
        }else {
          fcstweek
        }
        val fcstqty = 0.0d // 비어 있는 emptyYearweek 에 대해서 qty 값이 없기 때문에 0으로 채워줌
        var fcst = qtyMean // fcst 컬럼 추가
        Row( // Row 를 사용하면 아래 data 와 emptyMap 더해줄때 타입을 emptyMap.toIterable 식으로 바꿔줘야함
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨. -> 맞나 체크
          yearweek, // 비어있는 yearweek 가 들어옴
          fcstyear, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          fcstweek, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          fcstqty.toString, // 위에서 0으로 채워준거 적어줌
          fcst.toString
          //output : (noProductSeg1, noProductSeg2, noRegionSeg2, noRegionSeg3, noProductSeg3, noProductSeg3
        )
      })
      /////////////////////////////////////  미래주차 생성 ////////////////////////////////////
      fcstRow.toIterable ++ data
    })

//    finalRdd.take(10).foreach(println)


    //// 년도별 시즈널리티~~~ 평균~~~구해보기~~~

    var finalRdd2 = resultRdd.groupBy(x => {
      (
        x.getString(noRegionSeg1), // 미국
        x.getString(noProductSeg2), // 냉장고
        x.getString(noRegionSeg2), // 이마트
        x.getString(noRegionSeg3), // 플로리다
        x.getString(noWeek) //
      )
    }).map(x => {
      var key = x._1
      var data = x._2
      var size = data.size  // first 시, 5주차에 해당하는 3~4년치 qty 사이즈 3이 나옴

      var qtyList = data.map(x => { // QTY 에 해당하는놈만 qtyList 담는다
        x.get(noQty).toString.toDouble
      })
      // var qtyList = data.map(x=>{x.get(noQty).toString.toDouble})
      // 처음에 어떤 데이터타입이 들어올지 모르기 때문에, get 을 사용후, 원하는 데이터 산출을 위한 타입 캐스팅
      // 이렇게 하면 편하긴 하지만, 속도가 느리다는 단점이 있다.
      var qtyArray = qtyList.toArray
      var qtySum = qtyArray.sum // QTY 총합을 구함
      var qtyMean = if (size != 0) {
        qtySum / size // QTY 평균을 구함
      } else {
        0.0 // 분모가 0이면 0으로 해준다, 예외처리
      }
      (
        key,
        qtyMean
        )
    }).collectAsMap()  // .collectAsMap() 빼고 실행 후, 아래 step1 로직 실행뒤 resxx.count 하면 52주차 나온거 확인됨

    ///////////////////////////////////////////////////////////////////////////////////////////


    // A01,PG01,SALESID0001,SITEID0007,5
    // A01,PG05,SALESID0001,SITEID0003,10

    var step1 = finalRdd2.filter(x=>{   //  아래 4놈이 트루면, 52주차니까 4개가 튀어 나와야함.? 52 개가 아니라?
      var checkValid = false
      if((x._1._1 == "A01") &&
        (x._1._2 == "PG01") &&
        (x._1._3 == "SALESID0001") &&
        (x._1._4 == "SITEID0007")
//          (x._1._5 == "5")     // 여기까지 5개를 넣으면 1개가 나옴 ㅇㅇ
      ){
        checkValid = true
      }else{
        checkValid = false
      }
      checkValid
    })

    var step2 = step1.map(x=>{  // csv 파일로 내보낼때, 에러가 나서 이런 방법으로 진행을 함.
      (x._1._1,x._1._2,x._1._3,x._1._4,x._1._5, x._2)
    })

    var step3df = step2.toDF
    //    finalRdd.take(10).foreach(println)
    //    // 새로운 컬럼을 정의 해준다 여기서 컬럼의 이름은 finalRdd의 outputData값으로 #갯수도 일치 해야함(컬럼의갯수)
    //    var finalResult = finalRdd.toDF("REGIONSEG1","REGIONSEG2","REGIONSEG3","PRODUCTSEG1","PRODUCTSEG2","PRODUCTSEG3","YEARWEEK","YEAR","WEEK","QTY","SIZE","QTY_MEAN","STDEV","SEASONALITY")
    //
    //    finalResult.show
    //    // 저장단계
          step3df.
          coalesce(1). // 파일개수
          write.format("csv").  // 저장포맷
          mode("overwrite"). // 저장모드 append/overwrite
          option("header", "true"). // 헤더 유/무
          save("c:/spark/bin/data/18181818.csv") // 저장파일명


    ///////////////////////////////////////////////////////////////////////////////////////////


    var test = finalRdd.groupBy(x => { //  finalRdd 는 미래주차 생성후 -4 yearweek +4
          (
            x.getString(noRegionSeg1), // 미국
            x.getString(noProductSeg1), // 널값
            x.getString(noProductSeg2), // 냉장고
            x.getString(noRegionSeg2), // 이마트
            x.getString(noRegionSeg3), // 플로리다
            x.getString(noProductSeg3) // 냉장고001
          )
        }).flatMap(x => {

          var key = x._1  // key: (String, String, String, String, String, String) = (A01,null,PG05,SALESID0001,SITEID0001,ITEM0096)
          var data = x._2  // data.take(8).foreach(println)   24 ~ 31 주차 정보 나옴

      var weekArray = data.map(x=>{
        x.getString(noWeek)
      })

          var key1 = key._1  // A01
          var key2 = key._3  // PG05
          var key3 = key._4  // SALESID0001
          var key4 = key._5  // SITEID0001        주차별 시즈널리티 구할때 그룹바이 한애들을 key 값으로 가져옴

//      var value = 0d  // 초기화

          var ratio: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]()

          for (i <- weekArray) {   // 1~52주차 까지 돌려서
            var test = (key1, key2, key3, key4, i.toString)  // 해당 그룹바이(년주차평균) 로 구한 주차에 해당하는 qtyMean 가져옴
            if (finalRdd2.contains(test)) {  //
//              ratio ++= Array(finalRdd2(test))
              ratio(i.toString) = finalRdd2(test)
            }
          }
//      var forecast = 0.0d
//      val recentQtyLen = ratio.length
//      if (recentQtyLen > 0) {
//        val recentQtySum = ratio.sum
//        forecast = recentQtySum / recentQtyLen
//      }
      var result = data.map(x => {  // 여기부터 디버깅은 var result = data 하고, var x = result.head 로 시작
        val yearweek = x.getString(noYearweek)
        val year = x.getString(noYear)
        val week = x.getString(noWeek)
        val qty = x.get(noQty).toString.toDouble
        val fcst = x.getString(noQty + 1)
//        var fcst_time_series = 0.0d
//        var seasonality = 0.0d    // 24 25 26 27 주차 주차별 4년치 평균 시즈널리티 적어주기 위한 변수
//        var seasonality2 = 0.0d   // 28 29 30 31 주차 주차별 4년치 평균 시즈널리티 적어주기 위한 변수
//        var seasonality_Mean = 0.0d  // 24 25 26 27 주차 주차별 4년치 평균 시즈널리티 의 평균
//        if (ratio.contains(week)) {  // seasonality 에 8개의 값 각각 담음,
//          var seasonality = (Math.round(ratio(week)*100).toDouble)/100
//        }
        //  2. // seasonality 의 24 25 26 27 주차 값을 가져와서 평균을 냄
//          if ((ratio.contains(week)) && (week.toInt <= 27)) {
//            var seasonality = (Math.round(ratio(week)*100).toDouble)/100
//            var seasonality_Array = Array.empty[Int]  // 빈 배열만듬
//            seasonality_Array ++= Array(seasonality.toInt)
//            var seasonality_size = seasonality_Array.size   // 배열 사이즈
//            var seasonality_Sum = seasonality_Array.sum //
//            var seasonality_Mean = if (seasonality_size != 0) { // seasonality 의 24 25 26 27 값을 가져와서 평균을 냄
//              seasonality_Sum / seasonality_size //
//            } else {
//              0.0d
//            }
//          }else{
//              var seasonality = (Math.round(ratio(week)*100).toDouble)/100
//              fcst_time_series = if(seasonality_Mean != 0) {
//                ((fcst.toDouble)*(seasonality))/seasonality_Mean
//              }
//              else{
//                0.0d
//              }
//            }
            var weekArray1 = data.map(x=>{
                var seasonality = if ((ratio.contains(week)) && (week.toInt <= 27)) { // 24 25 26 27
                  (Math.round(ratio(week) * 100).toDouble) / 100
                }
                var seasonality_Array = Array.empty[Double]  // 빈 배열만듬
                seasonality_Array ++= Array(seasonality)
                var seasonality_size = seasonality_Array.size   // 배열 사이즈
                var seasonality_Sum = seasonality_Array.sum //
                var seasonality_Mean = if (seasonality_size != 0) {
                  seasonality_Sum / seasonality_size //
                }else{
                  0.0d
                }
                var fcst_time_series = if(seasonality_Mean != 0) {  // 미래주차아~
                  ((fcst.toDouble)*(seasonality))/seasonality_Mean
                }
                else{
                  0.0d
                }
            })

  //        두번째 테스트
//            var seasonality = if ((ratio.contains(week)) && (week.toInt <= 27)) { // 24 25 26 27
//              (Math.round(ratio(week)*100).toDouble)/100
//            } else {
//              (Math.round(ratio(week)*100).toDouble)/100  // 28 29 30 31
//            }
//            var seasonality_Array = Array.empty[Double]  // 빈 배열만듬
//            seasonality_Array ++= Array(seasonality)
//            var seasonality_size = seasonality_Array.size   // 배열 사이즈
//            var seasonality_Sum = seasonality_Array.sum //
//            var seasonality_Mean = if (seasonality_size != 0) {
//              seasonality_Sum / seasonality_size //
//            }else{
//              0.0d
//            }
//            var fcst_time_series = if(seasonality_Mean != 0) {  // 미래주차아~
//              ((fcst.toDouble)*(seasonality))/seasonality_Mean
//            }
//            else{
//              0.0d
//            }
          (
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨.
          yearweek,
          year,
          week,
          qty,
          fcst,
          seasonality,
//          seasonality2,
          fcst_time_series
        )
      })
      result
    })

//    test.take(10).foreach(println)

  }
}
