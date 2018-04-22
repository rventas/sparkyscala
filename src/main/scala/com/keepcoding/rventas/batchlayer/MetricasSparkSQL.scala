package com.keepcoding.rventas.batchlayer

import com.keepcoding.rventas.dominio.{Cliente, Geolocalizacion, Transaccion, Utilidades}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object MetricasSparkSQL {

  def run(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .appName("Practica final - Batch Layer - Saprk SQL")
      .master("local[*]")
      .getOrCreate

    import sparkSession.implicits._

    val rddTransacciones = sparkSession.read.csv(s"file:///${args(0)}")

    val cabecera = rddTransacciones.first

    val rddSincabecera = rddTransacciones.filter(!_.equals(cabecera)).map(_.toString().split(","))

    //Aproximacion 1, generar un dni secuencial mediante acumuladores => resultado, obtengo 998 clientes diferentes,
    //es decir, no se repite ningún cliente
    val acumulador: LongAccumulator = sparkSession.sparkContext.longAccumulator("dniCliente")
    val acumuladorTransaccion: LongAccumulator = sparkSession.sparkContext.longAccumulator("dniTransaccion")

    //TODO: Usar la librería de Google para sacar el país a partir de la longitud y la latitud
    val dfClientes = rddSincabecera.map(columna => {

      //Aproximacion 1:
      // Cliente(acumulador.value, columna(4), columna(6))

      //Para hacer que las metricas propuestas tengan mas sentido, voy a generar dni's en base al nombre, asignando
      //una cadena generada a partir de los caracteres del nombre.
      Cliente(Utilidades.generarDNI(columna(4)), columna(4), columna(6))

    })
    val dfTransacciones = rddSincabecera.map(func = columna => {
      //Aproximacion 1
      //acumuladorTransaccion.add(1)
      val descripcion = columna(10).replace("]", "")
      var categoriaCalc = "Otros"
      if (descripcion.indexOf("insura") != -1) { //Hay una errata en los datos, car insurace en lugar de car insurance
        categoriaCalc = "Seguros"
      } else if (descripcion.equals("Sports") || descripcion.equals("Restaurant") || descripcion.equals("Cinema")) {
        categoriaCalc = "Ocio"
      } else if (descripcion.equals("Shopping Mall")) {
        categoriaCalc = "Compras"
      }
      val fecha = Utilidades.transformarFecha(columna(0).replace("[",""))
      Transaccion(fecha, Utilidades.generarDNI(columna(4)), columna(2).toDouble, descripcion, categoriaCalc, columna(3),
        Geolocalizacion(columna(8).toDouble, columna(9).toDouble, columna(5).trim, "N/A"))
    })
    dfClientes.createOrReplaceGlobalTempView("CLIENTES")
    dfTransacciones.createOrReplaceGlobalTempView("TRANSACCIONES")

    //Para este dataset coincide que hay 765 nombres diferentes y se han generado 765 dni's distintos (de los 998 registros), así que doy por
    //buena la aproximación aunque el algoritmo de generacion de dnis unicos para nombres iguales no creo que garantice la unicidad
    val prueba = sparkSession.sql("SELECT count(*), count(distinct(cli.dni)) " +
      " FROM global_temp.CLIENTES cli ")

    prueba.show(1)


    //METRICAS
    //1. Contar el numero de transacciones por ciudad.
    val dfOutputMetrica1 = sparkSession.sql("SELECT tra.geolocalizacion.ciudad, count(*) " +
                                                     " FROM global_temp.TRANSACCIONES tra "+
                                                     "GROUP BY tra.geolocalizacion.ciudad")

    //2. Encuentra aquellos clientes que hayan realizado pagos superiores a 5000
    val dfOutputMetrica2 = sparkSession.sql("SELECT  distinct(cli.DNI) " +
                                                     "FROM global_temp.CLIENTES cli INNER JOIN global_temp.TRANSACCIONES tra ON cli.DNI = tra.DNI " +
                                                     "WHERE tra.importe > 5000")


    //3. Transacciones agrupadas por cliente cuya ciudad sea X
    val x: String = "London"
    val dfOutputMetrica3 = sparkSession.sql("SELECT tra.DNI, tra.geolocalizacion.ciudad " +
                                                     " FROM global_temp.TRANSACCIONES tra "+
                                                     " WHERE tra.geolocalizacion.ciudad = '" + x + "'" +
                                                     " GROUP BY tra.DNI, tra.geolocalizacion.ciudad ")


    //4. Filtra todas las transacciones cuya categoria sea Ocio
    val dfOutputMetrica4 = sparkSession.sql("SELECT tra.fecha, tra.DNI, tra.importe, tra.descripcion, tra.categoria," +
                                                    " tra.tarjetaCredito, tra.geolocalizacion.ciudad " +
                                                    " FROM global_temp.TRANSACCIONES tra "+
                                                    " WHERE tra.categoria = 'Ocio'")


    //5. Obten las transacciones en los ultimos 30 dias (como la transacciones son de enero de 2009, voy
    //a tomar para la metrica las de los últimos 15 días de enero)
    val dfOutputMetrica5 = sparkSession.sql("SELECT tra.fecha, tra.DNI, tra.importe, tra.descripcion, tra.categoria, " +
                                                     " tra.tarjetaCredito, tra.geolocalizacion.ciudad " +
                                                     " FROM global_temp.TRANSACCIONES tra " +
                                                     "WHERE to_date(tra.fecha)  > date_sub(current_timestamp(), 3383)")

    dfOutputMetrica1.write.csv(args(1)+"/metrica1")
    dfOutputMetrica2.write.parquet(args(1)+"/metrica2")
    dfOutputMetrica3.write.format("com.databricks.spark.avro").save(args(1)+"/metrica3")
    dfOutputMetrica4.write.json(args(1)+"/metrica4")
    dfOutputMetrica5.write.csv(args(1)+"/metrica5")

  }

}
