### Práctica del módulo Spark y Scala

* La ejecución de BatchApplication se hace con los siguientes argumentos de entrada:

	1. /home/keepcoding/KeepCoding/Workspace/PracticaRaquel/dataset/TransaccionesNew.csv 
	2. /home/keepcoding/KeepCoding/Workspace/PracticaRaquel/datasetOutput

* La ejecución de StreamingApplication requiere los siguientes argumentos de entrada:
	1. transacciones
	2. pagosSuperiores
	3. transaccionesCiudad
	4. transaccionesOcio
	5. ultimasTransacciones
	6. clientesConSaldoNegativo

  El primer argumento es el tópico producer de kafka y el resto son los tópicos consumer para cada una de las métricas
