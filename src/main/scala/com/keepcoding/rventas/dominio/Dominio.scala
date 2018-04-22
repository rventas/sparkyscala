package com.keepcoding.rventas.dominio

import java.util.Date

case class Geolocalizacion(latitud: Double, longitud: Double, ciudad: String, pais: String)
case class Transaccion(fecha: String, DNI: String, importe: Double, descripcion: String, categoria: String, tarjetaCredito: String,
                       geolocalizacion: Geolocalizacion)
case class Cliente(DNI: String, nombre: String, cuentaCorriente: String)
