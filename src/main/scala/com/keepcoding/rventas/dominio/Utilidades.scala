package com.keepcoding.rventas.dominio

object Utilidades {

  def generarDNI(nombre: String): String = {
    var dniString: String = ""
    for (i <- 0 to nombre.length - 1) dniString = dniString + scala.Char.char2int(nombre.charAt(i))
    dniString
  }

  def transformarFecha(fecha: String): String = {
    val indice1 = fecha.indexOf("/")
    val mes = fecha.substring(0, indice1)
    var restoFecha = fecha.substring(indice1+1, fecha.length)
    val indice2 = restoFecha.indexOf("/")
    val dia = restoFecha.substring(0, indice2)
    restoFecha = restoFecha.substring(indice2+1, restoFecha.length)
    val indice3 = restoFecha.indexOf(" ")
    val anio = restoFecha.substring(0,indice3)
    "20"+anio+"-"+ mes+"-"+dia
  }
}
