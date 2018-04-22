package com.keepcoding.rventas.speedlayer

import java.text.SimpleDateFormat
import java.util.Properties

import com.keepcoding.rventas.dominio.{Cliente, Geolocalizacion, Transaccion, Utilidades}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


object MetricasSparkStreaming {

  def run(args: Array[String]): Unit = {

    //Definición de la configuración de Spark
    val conf = new SparkConf().setMaster("local[*]").setAppName("Practica Final - Speed Layer")

    //Definición del nombre del tópico de Kafka del que consumiremos
    val inputTopic = args(0)

    //Definición de la configuración de Kafka, servidor, deseralizador, serializador etc
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )

    // Definición del contexto de spark streaming
    val ssc = new StreamingContext(conf, Seconds(5))

    //Definición del DStream
    val inputStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, String](Array(inputTopic), kafkaParams))

    //Guardado del valor consumido de Kafka
    val processedStream: DStream[String] = inputStream.map(_.value)

    //Muestro de los datos por pantalla
    //processedStream.print()


    val transaccion = processedStream.map(_.toString().split(","))
    val streamTransformado = transaccion.map(event => {
      val descripcion = event(10)
      var categoriaCalc = "Otros"
      if (descripcion.indexOf("insura") != -1) { //Hay una errata en los datos, car insurace en lugar de car insurance
        categoriaCalc = "Seguros"
      } else if (descripcion.equals("Sports") || descripcion.equals("Restaurant") || descripcion.equals("Cinema")) {
        categoriaCalc = "Ocio"
      } else if (descripcion.equals("Shopping Mall")) {
        categoriaCalc = "Compras"
      }
      new Tuple2(
        new Cliente(Utilidades.generarDNI(event(4)), event(4), event(6)),
        new Transaccion(event(0), Utilidades.generarDNI(event(4)), event(2).toDouble, descripcion, categoriaCalc, event(3),
          new Geolocalizacion(event(8).toDouble, event(9).toDouble, event(5), "N/A")))
    })


    //1. Agrupar todos los clientes por ciudad. El objetivo es contar todas las transacciones por ciudad.
    // Convert RDDs of the DStream to DataFrame and run SQL query
    streamTransformado.foreachRDD(foreachFunc = rdd => {

      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val clientesDataFrame = rdd.map(tupla => tupla._2).toDF()

      // Creates a temporary view using the DataFrame
      clientesDataFrame.createOrReplaceTempView("TRANSACCIONES")

      val clientesPorCiudad = spark.sql("SELECT tra.geolocalizacion.ciudad, count(*) " +
        "  FROM TRANSACCIONES tra " +
        " GROUP BY tra.geolocalizacion.ciudad")

      clientesPorCiudad.show()

    })

    //2. Encuentra aquellos clientes que hayan realizado pagos superiores a 5000
    val pagosSuperiores = streamTransformado.filter(_._2.importe > 5000)

    //3. Clientes cuya ciudad sea X
    val transaccionesCiudad = streamTransformado.filter(_._2.geolocalizacion.ciudad.equals("London"))

    //4. Filtra todas las transacciones cuya categoria sea Ocio
    val transaccionesOcio = streamTransformado.filter(_._2.categoria.equals("Ocio"))

    //5. Obten las transacciones en los ultimos 30 dias (como la transacciones son de enero de 2009, voy
    //a tomar para la metrica las de los últimos 15 días de enero)
    val sdf = new SimpleDateFormat("MM/dd/yy HH:mm")
    val fechaLimite = sdf.parse("01/15/2009 00:00")
    val ultimasTransacciones = streamTransformado.filter(tupla => sdf.parse(tupla._2.fecha).after(fechaLimite))

    //Metrica adicional 1: Detectar clientes con saldo negativo para poder avisarles de que su cuenta está al descubierto
    val clientesConSaldoNegativo = streamTransformado.filter(tupla => tupla._2.importe < 0)
    clientesConSaldoNegativo.foreachRDD(rdd => {
      rdd.saveAsTextFile("/home/keepcoding/KeepCoding/Workspace/PracticaRaquel/datasetOutput/metricaAdicional1")
    })

    //Metrica adicional 2: Detección de fraude. Contar las transacciones por ciudad de un cliente en un intervalo de
    //tiempo y si se producen transacciones en distintas ciudades mostrar una alerta
    val operacionesClienteCiudad = streamTransformado.map(tupla => ((tupla._2.geolocalizacion.ciudad, tupla._1.DNI),1))
      .reduceByKey(_+_)

    //Definición de la configuración del tópico de salida de Kafka
    val salidaMetrica1 = args(1)
    val salidaMetrica2 = args(2)
    val salidaMetrica3 = args(3)
    val salidaMetrica4 = args(4)

    //Creación del productor de Kafka que enviará la información procesada por SparkStreaming
    pagosSuperiores.foreachRDD(rdd => {
     rdd.foreachPartition(writeToKafka(salidaMetrica1))
    })
    transaccionesCiudad.foreachRDD(rdd => {
      rdd.foreachPartition(writeToKafka(salidaMetrica2))
    })
    transaccionesOcio.foreachRDD(rdd => {
      rdd.foreachPartition(writeToKafka(salidaMetrica3))
    })
    ultimasTransacciones.foreachRDD(rdd => {
      rdd.foreachPartition(writeToKafka(salidaMetrica4))
    })

    operacionesClienteCiudad.foreachRDD(rdd => {
      rdd.foreachPartition(writeToKafkaFraude("salidaFraude"))
    })
    ssc.start()
    ssc.awaitTermination()

  }

  def writeToKafka (outputTopic: String)(partitionOfRecords: Iterator[(Cliente, Transaccion)]): Unit = {
    val producer = new KafkaProducer[String, String](getKafkaConfig())
    partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
    producer.flush()
    producer.close()
  }

  def writeToKafkaFraude (outputTopic: String)(partitionOfRecords: Iterator[((String, String), Int)]): Unit = {
    val producer = new KafkaProducer[String, String](getKafkaConfig())
    partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic, data.toString())))
    producer.flush()
    producer.close()
  }

  //Definimos la configuración del kafka de salida
  def getKafkaConfig(): Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    properties
  }
}
