package com.ifchange.spark.streaming.v2.util

/**
  * Created by Administrator on 2018/1/9.
  */
class ParamParseUtil {


}

object ParamParseUtil {
  def parse(str: String): Map[String, String] = {
    var result = Map("" -> "")
    val split = str.split("&")
    if (split.length > 0) {
      for (text <- split) {
        val split1 = text.split("=")
        if (split1.length == 2) {
          val key = split1(0)
          val value = split1(1)
          result += (key -> value)
        }
        if (split1.length == 1) {
          val key = split1(0)
          val value = key
          result += (key -> value)
        }
      }
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val str = "t=2018-11-15 17:08:59&f=cv_entity&logid=3607802553&w=cv_entity&c=cv_entity&m=get_cvjd_fun_tags&s=1&r=4630"
    val map = ParamParseUtil.parse(str)
    val s = map.getOrElse("s","sss")
    print(s)

  }
}
