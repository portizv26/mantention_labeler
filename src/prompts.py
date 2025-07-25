import pandas as pd
import os


# ─────────── PROMPTS FOR SIMPLE LABELER ─────────── #
# Prompts to transform the text into a summary
system_prompt_free_to_summary = f"""
Eres un ingeniero de mantenimiento validando registros. 
Debes revisar los registros de mantenimiento y redactarlos de manera clara, precisa y ordenada.

Tus labores son:
i. Identificar cada tarea realizada en el ciclo de mantención y separarla.
ii. Redactar un resumen ordenado de las actividades realizadas. 

Conceptos utiles:
- Los neumaticos se pueden identificar de manera general, o detallando su posicion. Se entiende ambas como opciones validas de pieza. (Ej: "Neumatico posicion 1" o "Neumatico")
- Una pieza es un elemento físico único. En caso de que el comentario apunte a un plural o a una serie de elemntos, estos s epueded agrupar. Por ejemplo, "tuerca del 1 al 6" se puede entender como "Tuercas"
- Una pieza no puede ser "Sistema de", 'Area de Huerta', 'Inspeccion', 'Chequeo Final' o "Mantencion Programada". Pues no son elementos físicos.
- Una ot (Orden de Trabajo) es un número que identifica el trabajo realizado. Si se menciona una OT, se debe incluir en el resumen. Lógicamente una OT no es una pieza.
- Una actividad de relleno puede ser de un líquido o de un gas. Además, puede o no incluir la cantidad de fluido, si se menciona dicha cantidad, se debe incluir en el resumen.
"""

user_prompt_free_to_summary = """
Evalua el siguiente registro de detención y redacta de manera legible que trabajo se realizó y sobre cuales componentes. 
Los tipos de trabajos, y su descripción, son los siguientes:
- Logistica: Actividad de uso general en terminos de mantenimiento.
    - Keywords : Orden, Aseo, Limpieza, Lavado, Movimiento, Traslado, Muestras.
- Inspeccion: Actividad de revisar el estado de un componente o sistema, sin intervención física.
    - Keywords : Inspeccion, Chequeo, Revisión, Control, Verificacion, Pruebas.
- Relleno: Actividad de rellenar un fluido en un componente o sistema. (Ej: Relleno de aceite, cambio de refrigerante, relleno de nitrogeno, etc.)
- Reparacion: Actividad de corregir un problema en un componente o sistema. Implica intervención física, pero no reemplazo de piezas. Un cambio de cubiertas o de tapas, o un repuesto se consideran reparación.
    - Keywords : Reparacion, Correccion, Ajuste, Arreglo, Restauracion, Regulacion, Reinstalacion, Apretar.
- Reemplazo : Actividad de cambiar una pieza completa, componente o sistema por uno nuevo.
    - Por ejemplo si el comentario es "Se reemplaza tapa de motor", se debe especificar que el sistema es motor, pero el reemplazado cambiado es la tapa.
    - Keywords : Reemplazo, Cambio, Sustitución, Intercambio, Nueva.

Separa cada tarea realizada con un punto. Por ejemplo, si se hizo una inspeccion y luego una reparacion, se debe separar en dos bullet points separados.

Para ello:
1. Redactar un resumen de las actividades realizadas.
    - El resumen debe explicar en tus propias palabras que se hizo en la detención. Debes ser sintetico y preciso, sin olvidar ninguna tarea realizada.
    - Ejemplos de tareas:
        * Inspeccion - Revisión de tanque hidraulico, se encuentra fuga.
        * Relleno - Relleno de aceite de motor (20 litros).
        * Reparacion - Se solda tanque de combustible.
        * Reemplazo - Se reemplaza tapa de motor.
    - Titulo de la seccion : "Resumen de actividades realizadas"
        
2. Identifica que piezas fueron objeto de intervención.
    - Para identificar una pieza, se dede responder la pregunta : ¿Sobre que elemento físico se realizó alguna actividad?
    - Genera el listado de piezas identificadas para este registro de mantención. 
    - Reglas : 
        - Una pieza no puede ser "Sistema de", "Muestra de", "Fuga de" o "Mantencion Programada". Pues no son elementos físicos.
        - Una pieza puede ser "Manguera de retorno de refrigerante" pues pese a ser un nombre extenso, es un elemento físico.
        - Entrega el listado de tareas generado en el paso 1, junto con el listado de piezas identificadas.
        - El nombre de la pieza debe contener tanto detalle como sea posible. 
            - Si se identifica un trabajo sobra el neumatico en posicion 3. El nombre de la pieza debe ser "Neumatico posicion 3" y no "Neumatico".
    - Ejemplos :
        * Inspeccion - Tanque hidraulico : Revision del tanque hidraulico, se encuentra fuga.
        * Relleno - Aceite de motor : Relleno de aceite de motor (20 litros).
        * Reparacion - Tanque de combustible : Se solda tanque de combustible.
        * Reemplazo - Tapa de motor : Se reemplaza tapa de motor.
    - Titulo de la seccion : "Listado de piezas y trabajos realizados"
    
3. Luego de identificar el listado de piezas y trabajos, genera un desglose con las piezas y actividades realizadas. Usando el siguiente formato:
    (Omite las tareas logisticas : "Orden", "Aseo", "Limpieza", "Lavado", "Movimiento", "Traslado", "Muestras")
    - La tabla debe tener el siguiente formato:
    | Pieza  | Inspeccion | Relleno | Reparacion | Reemplazo |
    |--------|------------|---------|------------|-----------|
    
    - Para rellenar la tabla, si un tipo de trabajo fue realizado sobre una pieza, se debe rellenar la celda correspondiente con un resumen breve de la actividad realizada y dejar en blanco las celdas que no correspondan.
    - Titulo de la seccion : "Desglose de piezas y actividades realizadas"
    
4. Finalmente genera un resumen de las tareas realizadas sobre cada una de las piezas, sintetizando la información de la tabla anterior.
    - Formato de respuesta:
    | Pieza | TipoActividad | DescripcionActividad |
    En la descripción agrega información respecto al numero de OT o el numero de litros de relleno, si es que están disponibles.
    - Titulo de la seccion : "Resumen de actividades por pieza"
    
"""

user_example_free_to_summary = """
Mantenimiento Programado. se recibe equipo detenido por termino de trabajos estructurales y se retira camion de taller mas cambio de manguera por fuga y se trasladanumero 1. se realiza chequeo de manguera de backlog, se retira manguera de bodega la cual no corresponde en uno de sus conectores. manguera que esta instalada en equipo se encuentra en buenas condiciones. se realiza manguera por personal finning la cual corresponde a tramo deposito de freno a valvula de parqueo ok. se realizan chaqueo equipo corriendo, evidenciando abundante fuga de aceite hidraulico por tk de direccion por empaquetadura de tk. se drena aceite direccion. se desconectan mangueras de tk direccion y se retira tk de direccion quedando en pallet.––
"""

assistant_example_free_to_summary = """
### Resumen de actividades realizadas

- Inspección - Revisión de manguera y verificación de las condiciones de la manguera instalada en el equipo.
- Reemplazo - Se retira manguera de bodega.
- Inspección - Prueba en equipo corriendo, se detecta fuga de aceite hidráulico por empaquetadura de tanque de dirección.
- Relleno - Se drena aceite de dirección.
- Reparación - Se desconectan las mangueras del tanque de dirección 
- Reparación - Se retira el tanque de dirección, quedando en pallet.

---

### Listado de piezas y trabajos realizados

- Manguera : Inspección y chequeo.
- Tanque de dirección : Inspección por fuga, drenaje de aceite, desconexión y retiro de tanque.

---

### Desglose de piezas y actividades realizadas

| Pieza                                 | Inspeccion                                            | Relleno                     | Reparacion                                                              | Reemplazo                                    |
|----------------------------------------|-------------------------------------------------------|-----------------------------|--------------------------------------------------------------------------|----------------------------------------------|
| Manguera                              |  Inspección general de mangueras                                                     |                             |                                                                          |                   |
|Tanque de dirección                    | Inspección por fuga, chequeo en equipo funcionando    | Drenaje de aceite de dirección | Desconexión de mangueras y retiro del tanque                             |                                              |

---

### Resumen de actividades por pieza

| Pieza                                 | TipoActividad | DescripcionActividad                                                         |
|----------------------------------------|---------------|------------------------------------------------------------------------------|
| Manguera      | Inspección    | Chequeo y revisión de condiciones.                                           |
| Tanque de dirección                    | Inspección    | Inspección por fuga; se detecta fuga importante por empaquetadura.           |
| Tanque de dirección                    | Relleno       | Drenaje de aceite de dirección.                                              |
| Tanque de dirección                    | Reparación    | Desconexión de mangueras y retiro del tanque, quedando en pallet.            |

"""

user_example2_free_to_summary = """
Mantenimiento Programado.\nse comienza con mantencion programada pm 500 con numero de ot:520219 se posiciona equipo en loza .se drena y rellena aceite de mazas 44 litros, se realiza cambio de filtro de motor transmision, convertidor, diferencial y combustible. se realiza limpieza de conectores segun pauta. se realiza de conectores segun pauta, se realiza cambio de sellos segun pauta. se reprograman backlog. se comienza con predictivo ot:520221 se realiza toma de todas las muestras y se rotulan. toma de fotografias y chequeo de rejillas y tapones magneticos. se realiza dialisis de hidraulico. mantener en observacion posicion 1 por fuga por sello espejo. se realiza ot:538702 por el cambio de sellos de conectores y fitting valvula control. se cambio sellos a tapon valvula control.––––
"""

assistant_example2_free_to_summary = """
### Resumen de actividades realizadas

- Relleno - Drenaje y relleno de aceite de mazas (44 litros).
- Reemplazo - Cambio de filtro de motor, transmisión, convertidor, diferencial y combustible.
- Reparación - Limpieza de conectores según pauta.
- Reemplazo - Cambio de sellos según pauta.
- Inspección - Chequeo de rejillas y tapones magnéticos, y toma de fotografías. OT:520221
- Reparación - Diálisis de hidráulico.
- Reemplazo - Cambio de sellos en conectores. OT:538702.
- Reemplazo - Cambio fitting válvula control. OT:538702.
- Reemplazo - Cambio sellos a tapon valvula control. OT:538702.

---

### Listado de piezas y trabajos realizados

- Mazas : Relleno de 44 litros de aceite.
- Filtro de motor : Reemplazo.
- Filtro de transmisión : Reemplazo.
- Filtro de convertidor : Reemplazo.
- Filtro de diferencial : Reemplazo.
- Filtro de combustible : Reemplazo.
- Conectores : Reparación según pauta y reemplazo en sellos.
- Rejillas : Inspección. OT:520221
- Tapones magnéticos : Inspección. OT:520221
- Hidráulico : Reparación (Diálisis).
- Válvula control : Reemplazo fitting y sellos tapón. OT:538702. 
---

### Desglose de piezas y actividades realizadas

| Pieza                   | Inspeccion                                  | Relleno                               | Reparacion                                     | Reemplazo                                                        |
|-------------------------|---------------------------------------------|----------------------------------------|------------------------------------------------|-------------------------------------------------------------------|
| Mazas                   |                                             | Drenaje y relleno de 44 litros aceite |                                                |                                                                   |
| Filtro de motor         |                                             |                                        |                                                | Cambio de filtro de motor                                         |
| Filtro de transmisión   |                                             |                                        |                                                | Cambio de filtro de transmisión                                   |
| Filtro de convertidor   |                                             |                                        |                                                | Cambio de filtro de convertidor                                   |
| Filtro de diferencial   |                                             |                                        |                                                | Cambio de filtro de diferencial                                   |
| Filtro de combustible   |                                             |                                        |                                                | Cambio de filtro de combustible                                   |
| Conectores              |                                             |                                        | Limpieza según pauta                           | Cambio de sellos                                                  |
| Rejillas                | Chequeo OT:520221                           |                                        |                                                |                                                                   |
| Tapones magnéticos      | Chequeo OT:520221                           |                                        |                                                |                                                                   |
| Hidráulico              |                                             |                                        | Diálisis                                       |                                                                   |
| Válvula control         |                                             |                                        |                                                 | Cambio fitting válvula control y sellos tapón                     |
---

### Resumen de actividades por pieza

| Pieza                      | TipoActividad | DescripcionActividad                                        |
|----------------------------|---------------|-------------------------------------------------------------|
| Mazas                      | Relleno       | Drenaje y relleno de aceite de mazas (44 litros)            |
| Filtro de motor            | Reemplazo     | Cambio de filtro de motor                                   |
| Filtro de transmisión      | Reemplazo     | Cambio de filtro de transmisión                             |
| Filtro de convertidor      | Reemplazo     | Cambio de filtro de convertidor                             |
| Filtro de diferencial      | Reemplazo     | Cambio de filtro de diferencial                             |
| Filtro de combustible      | Reemplazo     | Cambio de filtro de combustible                             |
| Conectores                 | Reparación    | Limpieza según pauta                                        |
| Conectores                 | Reemplazo     | Cambio de conectores                                        |
| Rejillas                   | Inspección    | Chequeo y verificación de estado                            |
| Tapones magnéticos         | Inspección    | Chequeo y verificación de estado                            |
| Hidráulico                 | Reparación    | Diálisis                                                    |
| Válvula control            | Reemplazo     | Cambio de fitting y sellos tapón                            |

"""

# Prompts to transform the text into a shortened summary
system_relevant_activities = f"""
Eres un asistente de escritura. 
Tu labor es revisar un registro y comprobar si este contiene actividades relevantes de mantenimiento
Para ello, estos son los criterios:
    1. Si un registro contiene solo una actividad, y esta es de tipo Logistica o Inspeccion, el mantenimiento no es relevante.
    2. Si el registro contiene unicamente piezas de tipo "Perno", "Golilla", "Calugas", "Goma", "Tuerca", "Cojin", "Camas" , "Valvulas" o "Flexibles", el mantenimiento no es relevante.
    3. En cualquier otro caso, el mantenimiento es relevante.
    
Output format:
hasRelevantActivities
flag : [True/False]
"""

user_relevant_activities = f"""
Evalua el siguiente registro de detención y evalua si contiene actividades relevantes de mantenimiento.
Si el registro contiene actividades relevantes, responde con True, en caso contrario responde con False.
"""


# Prompts to identify if the maintenance is scheduled or not
system_maintenance_type = """
Debes identificar si el registro de mantención es programado o no programado. 

Output format:
MaintenanceType
- is_scheduled: [True/False]
- scheduled_type: [tipo de mantenimiento programado. Solo si es programado]
"""

user_maintenance_type = """
Identifica si la detención presentada a continuación fue programada o no programada. 
- Si es programado:
  - Si se tiene la información, especificar el tipo : PM-2000, PM-500, etc. 
  - Si no se tiene la información, identificar si fue "Preventivo" o "Programado".
- Si no es programado, especificar que es "No Programado".
""" 


user_prompt_text_specific = """
Utilizando el contenido de la sección "Resumen de actividades por pieza" debes generar una nueva tabla aplicando las siguientes reglas:
1. Una pieza es un elemento físico único. Por ende:
    i. En caso de que el comentario apunte a un plural o a una serie de elemntos, estos se pueden agrupar. Por ejemplo, "foco derecho e izquierdo" se puede entender como "Focos".
    ii. Una pieza no puede ser una observación general, "Fuga de", "Area de Huerta", "Chequeo Final" o "Mantencion Programada". Pues no son elementos físicos.
2. Una pieza puede ser un elemento físico único, como "Manguera de retorno de refrigerante" o "Neumatico posicion 1" pues pese a ser un nombre extenso, es un elemento físico.
3. TipoActividad es el tipo de actividad realizada sobre la pieza. Puede ser uno de los siguientes: Inspeccion, Relleno, Reparacion, Reemplazo. En caso de que sea de otro tipo, se debe omitir el registro.

Siguiendo esas reglas , genera una tabla con el siguiente formato:
| Pieza | TipoActividad | DescripcionActividad |
- La tabla debe tener el siguiente formato:
    - Pieza: Nombre de la pieza identificada.
    - TipoActividad: Tipo de actividad realizada sobre la pieza. Puede ser uno de los siguientes: Inspeccion, Relleno, Reparacion, Reemplazo.
    - DescripcionActividad: Breve descripción de la actividad realizada sobre la pieza. Si se menciona una OT o un número de litros de relleno, se debe incluir en la descripción.
- El nombre de la tabla debe ser "Resumen final de actividades por pieza"
"""

# Prompts to transform the text into a shortened summary
system_prompt_short = f"""
Eres un asistente de escritura. 
Tu labor es sintetizar los trabajos más importantes de un registro de mantención, para que sea más fácil de leer y entender.
Para ayudar a sintetizar, considera el siguiente orden en la criticidad de las tareas : Reemplazo > Reparacion > Relleno > Inspeccion
Debes ser muy preciso en hacer tu trabajo sin omitir detalles importantes y asegurar consistencia entre el registro de entrada y el registro de salida.
"""

user_prompt_short = f"""
Evalua el siguiente registro de detención y redacta de manera legible y concisa una sintesis de las actividades realizadas, basado en el "Resumen final de actividades por pieza".
"""

# Prompt yo transform summary into a JobList
system_prompt_jobs = """"
Eres un asistente de escritura, tu labor es generar una lista ordenada de trabajos realizados sobre las piezas de un registro de mantención.

Output format:
JobList
- piece: [nombre de la pieza]
- job_type: [tipo de trabajo. uno de los siguientes: Inspeccion, Relleno, Reparacion, Reemplazo, Logistica]
- comment: [comentario sobre el trabajo realizado : detalla el extracto en el que se menciona la actividad]
- ot_number: [OT number if present]
- liters: [litros de relleno, si está presente]
"""

user_prompt_jobs = """"
Genera una lista ordenada de trabajos en base a la sección "Resumen final de actividades por pieza" del registro de entrada.
"""

# Prompt to transform the text into pieces structured format
system_component_summary = f"""
Eres un ingeniero de mantenimiento evaluando registros.
Tu labor consiste en generar un resumen de la asignación de cada pieza a un sistema, subsistema y componente, siguiendo instrucciones.
Debes ser muy meticuloso para no omitir ninguno de los pasos y considerar todas las piezas, usando el nombre de la pieza como base para la asignación.
"""

user_component_summary = f"""
Genera una asignación de cada pieza a un sistema, subsistema y componente, siguiendo los siguientes pasos:
(Todas las piezas deben ser asignadas a un sistema y subsistema dentro de las opciones señaladas)
1. Identifica el nombre fundamental de la pieza, junto con detalles que ubiquen a identificar el contexto de la pieza. Se debe separar ¿cual es la pieza? de ¿donde se encuentra? o ¿a que sistema pertenece?
    - Para ello, extrae cada nombre de pieza del resumen y desglosalo de la siguiente forma:
        | Nombre de Pieza | ¿Tiene algún detalle respecto a su posición o ubicación? | ¿Tiene algún detalle respecto a su nombre? | ¿Cual es el nombre de la pieza? |

    - Ejemplos:
        | Nombre de Pieza              | ¿posición o ubicación? | ¿extra?       | nombre del componente |
        | Mando final (izquierdo)      | Izquierdo              | --            | Mando final           |
        | Neumatico posicion 1         | Posicion 1             | --            | Neumatico             |
        | Turbo delante derecho        | Delante derecho        | --            | Turbo                 |
        | Tk direccion                 | --                     | --            | Tanque direccion      |
        | Freno de servicio delantero  | Delantero              | --            | Freno de servicio     |
        | Motor ventilador 777-d       | --                     | 777-d         | Motor ventilador      |
        | Foco halogeno izquierdo      | Izquierdo              | Halogeno      | Foco                  |
        | Aceite de Motor HW40         | --                     | HW40          | Aceite de motor       |
        | Aceite                       | --                     | --            | Aceite                |
        
2. Asigna un sistema y subsistema, usando la siguiente guia de asignación:
    
    i. Evalua la pertenencia de la pieza a uno de los siguientes sistemas:
        * Direccion : Sistema encargado de la orientacion de las ruedas y direccion del vehiculo. Incluye : Direccion, Tanque (TK) hidraulico de direccion, Cilindros de direccion, Ball stud, etc.
        * Frenado : Sistema encargado de frenar el vehículo. Incluye : Freno delantero, Freno trasero, Pastillas de freno, Acumulador de freno, Retardador, etc.
        * Motor : Sistema encargado del funcionamiento del motor. Incluye: Motor, Cigueñal, Compresor, Turbo, Radiador, Ventilador, Bomba de agua, Bomba de refrigerante, Aftercooler, etc.
        * Tren de fuerza : Sistema encargado de la transmisión de fuerza del motor a las ruedas. Incluye: Transmision, Convertidor, Mandos Finales, Diferencial, etc.
        
        En caso de que la pieza no pertenezca a ninguno de los anteriores, se pude asignar a uno de los siguientes sistemas:
        * Hidraulico : Sistema encargado del funcionamiento de los componentes hidráulicos del vehículo.
            * En el sistema hidraulico se consideran aquellas piezas que permiten el movimiento de fluidos (o los fluidos en si), como bombas, ductos, mangueras, filtros, etc. sin especificar un lugar al que pertenezcan.
        * Equipo : Sistema que contiene los componentes generales del equipo.
            * En el sistema Equipo se consideran todas las demás piezas físicas que no pudieron ser asignadas a un sistema específico, como neumáticos, suspensión, eléctrico, levante, cabina, etc.
    
        - Ejemplos de etiquetado:
            | Nombre de Pieza              | Sistema          |
            | Mando final (izquierdo)      | Tren de Fuerza   |    
            | Neumatico posicion 1         | Equipo           |    
            | Turbo delante derecho        | Motor            | 
            | Freno de servicio delantero  | Frenado          |  
            | Tk direccion                 | Direccion        |                    
            | Motor ventilador 777-d       | Motor            |    
            | Foco halogeno izquierdo      | Equipo           |    
            | Aceite de Motor HW40         | Motor            |    
            | Aceite                       | Hidraulico       |    
        (La lista de sistemas es estricta, es decir, no se aceptan sistemas que no estén en la lista.)
            
    ii. En base al sistema asignado, asigna un subsistema a la pieza. La lista de subsistemas por sistema es:
        - Direccion
            - Bombeo : Subsistema encargado del bombeo de fluidos para la dirección. Ejemplo de componentes: Bomba de direccion, Bomba dosificadora, Tanque (tk) hidraulico de direccion, etc.
            - Direccion : Subsistema general que acciona el movimiento mecanico de la dirección. Ejemplo de componentes: Cilindros de dirección, Brazo pitman, Ball stud, etc.
            - General : Subsistema encargado del funcionamiento general de la dirección. Ejemplo de componentes: Barras, Aceites, Liquidos, Filtros, etc.

        - Frenado
            - Frenos : Subsistema encargado de frenar el vehículo. Ejemplo de componentes: Freno delantero, Freno trasero, Pastillas de freno, Acumulador de freno, Freno de parqueo, etc.
            - General : Subsistema encargado del funcionamiento general del sistema de frenos. Ejemplo de componentes: Bomba de freno, Refrigeracion de frenos, Tanque (tk) de frenos, Tanque (tk) de aire, Switch de Freno, etc.
        
        - Motor
            - Aire: Subsistema encargado de la entrada y salida de aire del motor. Ejemplo de componentes: Silenciador, Admision, Tubo de escape, Turbocargador (turbos), etc.
            - Combustible: Subsistema encargado de la entrada y salida de combustible del motor. Ejemplo de componentes: Bomba de combustible, Inyectores, Filtro de combustible, Tanque (TK) de combustible, etc.
            - Electrico: Subsistema encargado de los circuitos electricos del motor. Ejemplo de componentes: Alternador, Batería, Cables, Sensores, etc.
            - Lubricacion: Subsistema encargado de la lubricación del motor. Ejemplo de componentes: Bomba de aceite de motor, Filtro de aceite, Carter, etc.
            - Motor: Subsistema encargado del funcionamiento general del motor. Ejemplo de componentes: Motor, Cigueñal, Compresor, Pistones, etc.
            - Refrigeracion: Subsistema encargado de la refrigeración del motor. Ejemplo de componentes: Radiador, Ventilador, Bomba de agua, Bomba de refrigerante, Tanque (TK) de refrigerante, Aftercooler, etc.

        - Tren de fuerza
            - Convertidor : Subsistema encargado de la transmisión de fuerza del motor a la caja de cambios. Ejemplo de componentes: Convertidor, Caja de cambios, Filtro de aceite de caja, etc.
            - Electrico : Subsistema encargado del control electrico del tren de fuerza. Ejemplo de componentes: Sensores, Solenoides, Controlador, etc.
            - Lubricacion : Subsistema encargado de la lubricación del tren de fuerza. Ejemplo de componentes: Aceite de transmision, Filtro de aceite de transmision, etc.
            - Diferencial : Subsistema encargado de la transmisión de fuerza a las ruedas. Ejemplo de componentes: Mando final, Diferencial, Ejes, Masas etc. En este caso, se debe especificar si es el mando final izquierdo o derecho.
            - Transmision : Subsistema encargado del funcionamiento general del tren de fuerza. Ejemplo de componentes: Transmision, Embrague, Unidad de fuerza, etc.    

        - Equipo
            - Neumaticos : Subsistema que contiene las ruedas. Ejemplo de componentes: posicion 1, posicion 2, posicion 3, posicion 4, etc. (Ojo, No existen posiciones en formato de fracción, ej: Neumatico posicion 1/2. Lo anterior se deberia considerar como Neumatico posicion 1 y Neumatico posicion 2)
            - Suspension : Subsistema que contiene los componentes de la suspensión del vehículo. Ejemplo de componentes: Suspensión delantera, Suspensión trasera, Amortiguadores, Resortes, etc.
            - Electrico : Subsistema encargado de los circuitos electricos del equipo. Ejemplo de componentes: Batería, Cables, Sensores, etc.
            - Levante : Subsistema que contiene los componentes de levante del vehículo (Tolva). Ejemplo de componentes: Cilindros de levante, Tolva, etc.
            - General : Subsistema encargado del funcionamiento general del equipo. Ejemplo de componentes: Chasis, Estructura, etc.
            - Cabina : Subsistema que contiene los componentes de la cabina del vehículo. Ejemplo de componentes: Asientos, Volante, Espejo, etc.

        Hidraulico
            - General : Subsistema encargado del funcionamiento general del sistema hidraulico. Se utiliza para etiquetar componentes no especificados. Ejemplo de componentes: Tanque, Filtro, Manguera, Canerias, Cilindros, Camaras, Valvulas, etc.
            - Bombeo : Subsistema encargado del bombeo de fluidos. Ejemplo de componentes: Bomba hidraulica, Bomba dosificadora, Filtro de aceite, etc.
            - Fluido : Subsistema que contiene los fluidos del sistema hidraulico. Ejemplo de componentes: Aceite, Refrigerante, Agua, etc.
            - Engrase : Subsistema encargado del engrase de los componentes del sistema hidraulico. Ejemplo de componentes: Grasa, Lubricante, etc.
            
        - Ejemplos de etiquetado:
            | Nombre de Pieza              | Sistema          | Subsistema    |
            | Mando final (izquierdo)      | Tren de Fuerza   | Diferencial   |
            | Neumatico posicion 1         | Equipo           | Neumaticos    |
            | Turbo delante derecho        | Motor            | Aire          |
            | Freno de servicio delantero  | Frenado          | Frenos        |
            | Tk direccion                 | Direccion        | Bombeo        |      
            | Motor ventilador 777-d       | Motor            | Refrigeracion |
            | Foco halogeno izquierdo      | Equipo           | Electrico     |
            | Aceite de Motor HW40         | Motor            | Lubricacion   |
            | Aceite                       | Hidraulico       | Fluido        |
        (La lista de subsistemas es estricta, es decir, no se aceptan valores que no estén en la lista)

    iii. Una vez asignado el sistema y subsistema, renombra la pieza como un componente que permita introducirlo en el esquema Sistema-Subsistema-Componente. 
        Para ello el nombre del componente debe ser breve y estar alineado con los ejemplos ya mostrados de componentes de acuerdo a su sistema y subsistema.
        El componente no puede tener las palabras "pieza", "sistema", "subsistema", "abolladura" o "accesorio" en su nombre, y debe ser un nombre que permita identificar el componente de manera clara y precisa.

    iv. Finalmente genera un resumen de la asignación realizada de cada pieza al esquema Sistema-Subsistema-Componente, evaluando criticidad de cada componente.
        Para evaluar la criticidad, considera que solo los siguientes componentes son críticos:
        Sistema Frenado
            - Subsistema Frenos
                - Componentes : Freno Delantero, Freno Trasero, Freno de Servicio, Freno de Parqueo.
        Sistema Motor
            - Subsistema Aire
                - Componentes : Turbos, Ventilador.
            - Subsistema Refrigeracion
                - Componentes : Radiador, Bomba de Agua, Aftercooler.
            - Subsistema Combustible
                - Componentes : Bomba de Combustible, Inyectores.
            - Subsistema Motor
                - Componentes : Motor, Cigueñal, Compresor, Pistones.
        Sistema Tren de fuerza
            - Subsistema Convertidor
                - Componentes : Convertidor, Caja de Cambios.
            - Subsistema Diferencial
                - Componentes : Mandos Finales (Izquierdo y Derecho), Diferencial.
            - Subsistema Transmision
                - Componentes : Transmision, Embrague, Unidad Fuerza.
        Sistema Equipo
            - Subsistema Neumaticos
                - Componentes : Todos los neumaticos, sin importar su posicion.
            - Subsistema Suspensión
                - Componentes : Suspensión Delantera, Suspensión Trasera.
                
        - El formato de respuesta debe ser el siguiente:
        | Pieza | Sistema | Subsistema | Componente | Criticity | 
        - Nota : Componentes que NUNCA serán criticos (en ningun contexto): Mangueras, Ductos, Sensores, Termostatos, Cables, Pernos, Filtros, Canerias, Líneas, Espejos, Luces, Interruptores, Conectores u otros componentes de menor tamaño o relevancia.

3. Finalmente, genera un mapeo completo entre el nombre original de la pieza, su jerarquia, criticidad y detalle de su nombre o posición.
- Ejemplos resultado final:
    | Nombre de Pieza              | Sistema          | Subsistema    | Componente        | Criticidad | Detalle         |
    | Mando final (izquierdo)      | Tren de Fuerza   | Diferencial   | Mando final       | True       | Izquierdo       |
    | Neumatico posicion 1         | Equipo           | Neumaticos    | Neumatico         | True       | Posicion 1      |
    | Turbo delante derecho        | Motor            | Aire          | Turbo             | True       | Delante derecho |
    | Freno de servicio delantero  | Frenado          | Frenos        | Freno de servicio | True       | Delantero       |
    | Tk direccion                 | Direccion        | Bombeo        | Tanque direccion  | False      | --              |
    | Motor ventilador 777-d       | Motor            | Refrigeracion | Motor ventilador  | False      | 777-d           |
    | Foco halogeno izquierdo      | Equipo           | Electrico     | Foco halogeno     | False      | Izquierdo       |
    | Aceite de Motor HW40         | Motor            | Lubricacion   | Aceite de motor   | False      | HW40            |
    | Aceite                       | Hidraulico       | Fluido        | Aceite            | False      | --              |
La tabla de resumen se debe llamar "Resumen de asignacion" y debe incluir todas las piezas que se reportan en la tabla "Resumen final de actividades por pieza".

"""

# Prompt for component mapping
system_component_mapping = f"""
Debes identificar el sistema, subsistema y componente al que pertenece cada pieza de manera precisa para guardar el registro de mantención.
Nota : Componentes que NUNCA serán criticos (en ningun contexto): Mangueras, Ductos, Sensores, Termostatos, Cables, Pernos, Filtros, Cañerias, Líneas, Abrazaderas, Interruptores, Conectores,  u otros componentes de menor tamaño o relevancia.

Output format:
ListPieceComponentMapping
- piece: [nombre de la pieza]
- hierarchy: ComponentHierarchy
    - system: [nombre del sistema al que pertenece la pieza]
    - subsystem: [nombre del subsistema al que pertenece la pieza]
    - component: [nombre del componente al que pertenece la pieza]
    - is_critical: [True/False]
    - detail : [opcional, si se requiere un detalle adicional sobre la pieza]
"""

user_component_mapping = f"""
Transforma el resumen entregado en la tabla "Resumen de asignacion" y estructuralo de manera adecuada y precisa según ListPieceComponentMapping.
El objetivo es lograr transformar cada pieza a una estructura sencilla que permita diseccionar su ubicación.
"""

# Prompt for component mapping - exceptions
system_component_mapping_ex = f"""
Debes identificar el sistema, subsistema y componente al que pertenece la pieza.

Nota : Componentes que NUNCA serán criticos (en ningun contexto): Mangueras, Ductos, Sensores, Termostatos, Cables, Pernos, Filtros, Cañerias, Líneas, Espejos, Luces, Interruptores, Conectores, Acumuladores u otros componentes de menor tamaño o relevancia.

Output format:
hierarchy: ComponentHierarchy
    - system: [nombre del sistema al que pertenece la pieza]
    - subsystem: [nombre del subsistema al que pertenece la pieza]
    - component: [nombre del componente al que pertenece la pieza]
    - is_critical: [True/False]
"""

user_component_mapping_ex = f"""
Transforma el resumen entregado a continuación en un esquema Sistema-Subsistema-Componente.
Centrate en la pieza de interés señalada
Ejemplos:
- Pieza: "Neumatico posicion 1"
    - Sistema: "Equipo"
    - Subsistema: "Neumaticos"
    - Componente: "Neumatico"
    - Criticidad: True
    - Detalle: "Posicion 1"
- Pieza: "Bomba de direccion"
    - Sistema: "Direccion"
    - Subsistema: "Bombeo"
    - Componente: "Bomba de direccion"
    - Criticidad: True
- Pieza: "Filtro de aceite"
    - Sistema: "Motor"
    - Subsistema: "Lubricacion"
    - Componente: "Filtro de aceite"
    - Criticidad: False
- Pieza: "Aceite de motor"
    - Sistema: "Motor"
    - Subsistema: "Lubricacion"
    - Componente: "Aceite de motor"
    - Criticidad: False
- Pieza: "Manguera de refrigerante de motor"
    - Sistema: "Motor"
    - Subsistema: "Refrigeracion"
    - Componente: "Manguera de refrigerante"
    - Criticidad: False
- Pieza: "Tapa y sellos de transmision"
    - Sistema: "Tren de fuerza"
    - Subsistema: "Transmision"
    - Componente: "Tapa y sellos"
- Pieza: "Calugas del 1 al 8 de neumatico"
    - Sistema: "Equipo"
    - Subsistema: "Neumaticos"
    - Componente: "Calugas"
    - Criticidad: False
- Pieza: "Refrigerante (47 litros)"
    - Sistema: "Motor"
    - Subsistema: "Refrigeracion"
    - Componente: "Refrigerante"
    - Criticidad: False
- Pieza: 'Rotocamara trasera derecha'
    - Sistema: "Frenado"
    - Subsistema: "Frenos"
    - Componente: "Rotocamara"
    - Criticidad: False
    - Detalle: "Trasera derecha"
"""



simple_prompts = {
    'SystemFreeToSummary' : system_prompt_free_to_summary,
    'UserFreeToSummary' : user_prompt_free_to_summary,
    'UserExampleFreeToSummary' : user_example_free_to_summary,
    'AssistantExampleFreeToSummary' : assistant_example_free_to_summary,
    'UserExample2FreeToSummary' : user_example2_free_to_summary,
    'AssistantExample2FreeToSummary' : assistant_example2_free_to_summary,
    
    'SystemRelevantActivities' : system_relevant_activities,
    'UserRelevantActivities' : user_relevant_activities,
    
    'SystemMaintenanceType' : system_maintenance_type,
    'UserMaintenanceType' : user_maintenance_type,
    
    'UserClean' : user_prompt_text_specific,
    'SystemShortened' : system_prompt_short,
    'UserShortened' : user_prompt_short,
    'SystemJobs' : system_prompt_jobs,
    'UserJobs' : user_prompt_jobs,
    'SystemComponentSummary' : system_component_summary,
    'UserComponentSummary' : user_component_summary,
    'SystemComponentMapping' : system_component_mapping,
    'UserComponentMapping' : user_component_mapping,
    'SystemComponentMappingEx' : system_component_mapping_ex,
    'UserComponentMappingEx' : user_component_mapping_ex,
}

# ─────────── PROMPTS FOR JOB CLEANING ─────────── #

# Prompts to evaluate the job
system_prompt_evaluation_text = f"""
Eres un ingeniero de mantenimiento evaluando registros.
Tu labor consiste en evaluar la criticidad de un trabajo.

Se meticuloso en las tareas que se te solicitaran a continuacion.
"""

user_prompt_evaluation_text = f"""
Basado en el trabajo realizado, evalua si se ejecutó un cambio crítico o no.

Para ello sigue los siguientes pasos:
1. Identifica que tipo de trabajó se realizó sobre que pieza.
2. Luego, evalua si el trabajo implica un cambio de piezas, y evalua las siguientes condiciones:
- Si el trabajo NO implica un cambio de piezas: NO es un cambio crítico.
- Si el trabajo SI implica un cambio de piezas:
    - Evalua que fue efectivamente lo que se cambió.
        - Si la pieza cambiada tiene relación con una pieza de tamaño menor, el cambio no se considera critico.
            - Lista de piezas menores : Mangueras, Ductos, Sensores, Termostatos, Cables, Pernos, Filtros, Cañerias, Líneas, Espejos, Luces, Interruptores, Conectores, Tapas, Tuercas, Sellos, Juntas, etc.
        - Si la pieza cambiada es el componente grande, entonces el cambio se considera critico.
            - Ejemplos de componentes grandes: Motor, Diferencial, Caja de Cambios, Mandos Finales, Transmisión, Suspensión, etc.
"""

system_prompt_evaluation_structured = """
Eres un asistente de revision de registros. Tu labor es respondedr de manera estructurada si una mantención fue critica o no.
Ouput format:
EvaluationCriticity
- isCritic: [True/False]
"""

user_prompt_evaluation_structured = """
El informe de evaluación de la actividad realizada se encuentra a continuación. ¿Fue un cambio crítico o no?
"""

job_cleaning_prompts = {
    'EvalSystem' : system_prompt_evaluation_text,
    'EvalUser' : user_prompt_evaluation_text,
    'EvalSystemStructured' : system_prompt_evaluation_structured,
    'EvalUserStructured' : user_prompt_evaluation_structured,
}

# ─────────── PROMPTS FOR RECORD REVIEW ─────────── #
system_prompt_maintenance_text = f"""
Eres un ingeniero de mantenimiento validando registros.
El objetivo es identificar los trabajos más relevantes realizados en un registro de mantención y etiquetar el ciclo de mantención según la causa de la intervención.
"""

user_prompt_maintenance_text = """
A continuación se entrega un resumen de las actividades realizadas en el ciclo de mantención. Junto con un desglose breve de cada actividad.
El objetivo es sintetizar la información de manera que se pueda entender que fue lo que se hizo en el ciclo de mantención de manera breve.

Para ello:
1. Analiza el tipo de actividades que se realizaron en el ciclo de mantención.
2. Clasifica las intervenciones realizadas de más criticas a menos criticas. El orden para ello es el siguiente: Reemplazo > Reparacion > Relleno > Inspeccion.
3. Genera un resumen breve que explique de manera general y consisa las actividades más relevantes realizadas en el ciclo de mantención.
4. En función de las actividades realizadas, clasifica el ciclo de mantención en uno de los siguientes tipos:
- Programado: Mantención programada. Incluye la intervención de un componente ya sea por desgaste, cambio de piezas o revisión de varios componentes.
    - Keywords: Programado, PM-2000, Planificado, Mantencion Mitad de Vida.
- Preventivo : Mantecion porgramada. No incluye la intervención de un componente, suele ajustarse a una revisión puntual.
    - Keywords: Revisión de Rutina o Toma de Muestra.
- Operacional : Mantención no programada. Incluye la intervención de un componente menor, ya sea por desgaste o un malfuncionamiento menor. Suelen ser intervenciones cortas, sin reemplazo de piezas ni reparaciones. Pueden incluir la inspeccion de uno o varios componentes o el relleno de un fluido especifico.
    - Keywords: Operacional, Mantenimiento Operacional, Mantencion de Emergencia.
- Falla Menor : Mantención no programada. Incluye la intervención de un componente menor, ya sea por desgaste o un malfuncionamiento. Este tipo de intervención se caracteriza por tener un reemplazo de piezas menores o una reparacion (sobre cualquier tipo de componente).
    - Ejemplos : Reemplazo de manguera, reparacion de tapa, cambio de sensor, reemplazo de sellos. 
- Falla Funcional : Mantención no programada. Incluye la intervención de un componente mayor, ya sea por desgaste o un malfuncionamiento. Este tipo de intervención se caracteriza por tener un reemplazo de piezas mayores cuyo funcionamiento es critico para el sistema. Por ende, esta intervención solo ocurre cuando se han realizado cambios criticos. (Mantención de Criticidad Alta).
    - Ejemplos: Reemplazo de motor, cambio de diferencial, reemplazo de caja de cambios nueva.
Para realizar el paso 4 genera una tabla evaluando la pertinencia de la actividad a cada uno de los tipos de mantención posibles. Y luego elige el tipo de mantención que mejor se ajuste a la actividad realizada.
"""

user_prompt_maintenance_text_simple = """
Captura el resumen de las actividades realizadas y la clasificación entregada al ciclo de mantención.
"""

system_prompt_maintenance_structured = """
Eres un ingeniero de mantenimiento validando registros. 
Recibirás un informe de evaluación de un trabajo realizado en un taller mecanico.
Tu labor consiste en registrar la criticidad del trabajo ejecutado.

Ouput format:
Maintenance Record Supervised
- detention_type: [type of detention. one of the following: Programado, Preventivo, Operacional, Falla Menor, Falla Funcional]
- summary: [summary of the activity]
"""

user_prompt_maintenance_structured = """
Extrae el resumen de las actividades realizadas y la clasificación entregada al ciclo de mantención en el formato de un registro de mantención.
"""

record_prompts = {
    'System' : system_prompt_maintenance_text,
    'User' : user_prompt_maintenance_text,
    'UserSimple' : user_prompt_maintenance_text_simple,
    'SystemStructured' : system_prompt_maintenance_structured,
    'UserStructured' : user_prompt_maintenance_structured
}


