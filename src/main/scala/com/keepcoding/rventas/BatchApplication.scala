package com.keepcoding.rventas

import com.keepcoding.rventas.batchlayer.MetricasSparkSQL


object BatchApplication {

  def main(args: Array[String]): Unit = {

     if (args.length == 2) {
       MetricasSparkSQL.run(args)
     } else {
       println("Se está intentando arrancar el job de Spark sin los parámetros correctos")
     }
  }
}
