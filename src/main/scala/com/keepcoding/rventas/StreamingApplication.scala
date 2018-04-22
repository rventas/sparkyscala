package com.keepcoding.rventas

import com.keepcoding.rventas.speedlayer.MetricasSparkStreaming

object StreamingApplication {

    def main(args: Array[String]): Unit = {

      if (args.length == 6) {
        MetricasSparkStreaming.run(args)
      } else {
        println("Se está intentando arrancar el job de Spark sin los parámetros correctos")
      }
    }

}
