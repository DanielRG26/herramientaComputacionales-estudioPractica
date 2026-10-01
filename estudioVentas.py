from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("datasets/EstudioPySpark") \
    .master("local[*]") \
    .getOrCreate()

clientes = spark.read.csv(
    "datasets/clientes.csv",
    header=True,
    inferSchema=True
)

productos = spark.read.csv(
    "datasets/productos.csv",
    header=True,
    inferSchema=True
)

productos.createOrReplaceTempView("productos")

ventas = spark.read.csv(
    "datasets/ventas.csv",
    header=True,
    inferSchema=True
)

# Mostrar los datos de clientes
# clientes.show()

# Mostrar los datos de ventas
# ventas.show()

# Mostrar los datos de productos
# productos.show()


ventasClientes = ventas.join(
    clientes,
    ventas.id_cliente == clientes.id_cliente,
    "inner"
)

# Sirve para unir las ventas con los clientes y obtener información de ambos.
# ventasClientes.select("id_venta","nombre","id_Producto").show()


ventas_completas = ventas \
    .join(
        clientes,
        ventas.id_cliente == clientes.id_cliente,
        "inner"
    ) \
    .join(
        productos,
        ventas.id_producto == productos.id_producto,
        "inner"
    ).select(
        ventas.id_venta,
        ventas.fecha,
        ventas.id_cliente,
        clientes.nombre,
        clientes.ciudad,
        ventas.id_producto,
        productos.producto,
        productos.categoria,
        productos.precio,
        ventas.cantidad,
        ventas.metodo_pago,
        ventas.estado
    )

# Sirve para unir las ventas con clientes y productos en una sola tabla.
ventas_completas.createOrReplaceTempView("ventasCompletas")

# Mostrar información de las ventas completas
# ventas_completas.select("id_venta","nombre","producto","cantidad","estado").show()


ventas_completas = ventas_completas.withColumn(
    "total",
    col("cantidad") * col("precio")
)

# Sirve para calcular el valor total de cada venta.
# ventas_completas.select("id_venta","nombre","producto","cantidad","precio","total","estado").show()


ventana = Window \
    .partitionBy("id_cliente") \
    .orderBy("fecha") \
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )

ventas_completas.withColumn(
    "total_acumulado",
    sum("total").over(ventana)
).select(
    "id_cliente",
    "fecha",
    "total",
    "total_acumulado"
)#.show()

# Sirve para calcular el total acumulado de compras por cliente.


# Sirve para contar el total de ventas.
# spark.sql("""select count(*) from ventasCompletas""").alias("cantidadVentas").show()

# Sirve para contar el total de productos.
# spark.sql("""select count(*) from productos""").alias("cantidadProductos").show()

# Sirve para contar las ventas que no fueron canceladas.
# spark.sql("""select count(*) from ventasCompletas as v where v.estado != "Cancelada" """).alias("totalVentasCompletadas").show()

# Sirve para calcular el precio promedio de los productos.
# productos.agg(round(avg("precio")).alias("precioPromedioProductos")).show()

# Sirve para sacar el producto más caro.
# productos.select("producto","precio").orderBy(col("precio").desc()).show(1)

# Sirve para sacar el producto más barato.
# productos.select("producto","precio").orderBy(col("precio").asc()).show(1)

# Sirve para calcular el total de unidades vendidas.
# ventas_completas.agg(sum("cantidad").alias("cantidadUnidadesVendidas")).show()

# Sirve para calcular el promedio de unidades por venta.
# ventas_completas.agg(avg("cantidad").alias("promedioUnidadesVenta")).show()

# Sirve para contar las ventas completadas.
# spark.sql("""select count(*) from ventasCompletas as v where v.estado == "Completada" """).alias("totalVentasCompletadas").show()

# Sirve para contar las ventas canceladas.
# spark.sql("""select count(*) from ventasCompletas as v where v.estado == "Cancelada" """).alias("totalVentasCompletadas").show()

# Sirve para saber cuántas ventas realizó cada cliente.
# spark.sql(""" select nombre, count(*) as cantidadVentas from ventasCompletas group by nombre """).show()

# Sirve para saber cuántas ventas tiene cada producto.
# spark.sql(""" select producto,count(*) as cantidadVendido from ventasCompletas group by producto""").show()

# Sirve para saber cuántas ventas se realizaron por ciudad.
# spark.sql(""" select ciudad,count(*) as cantidadVendido from ventasCompletas group by ciudad""").show()

# Sirve para calcular el precio promedio de cada categoría.
# productos.groupBy("categoria").agg(round(avg("precio")).alias("precioPromedio")).orderBy(col("precioPromedio").desc()).show()

# Sirve para sacar el producto con mayor cantidad de ventas.
# spark.sql(""" select producto,count(*) as cantidadVendido from ventasCompletas group by producto order by cantidadVendido desc""").show(1)

# Sirve para sacar el cliente con mayor cantidad de compras.
# spark.sql(""" select nombre,count(*) as cantidadVendido from ventasCompletas group by nombre order by cantidadVendido desc""").show(1)

# Sirve para sacar la ciudad con mayor cantidad de ventas.
# spark.sql(""" select ciudad,count(*) as cantidadVendido from ventasCompletas group by ciudad order by cantidadVendido desc""").show(1)

# Sirve para sacar el método de pago más utilizado.
# spark.sql(""" select metodo_pago,count(*) as cantidadVendido from ventasCompletas group by metodo_pago order by cantidadVendido desc""").show(1)

# Sirve para mostrar la cantidad de ventas realizadas por cada método de pago.
# spark.sql(""" select metodo_pago,count(*) as cantidadVendido from ventasCompletas group by metodo_pago """).show()

# Sirve para sacar la categoría con mayor precio promedio.
ventas_completas.groupBy("categoria").agg(round(avg("precio")).alias("precioPromedio")).orderBy(col("precioPromedio").desc()).show(1)