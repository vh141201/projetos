import kagglehub
from pyspark.sql import SparkSession

path = r"C:\Users\Victor\.cache\kagglehub\datasets\olistbr\brazilian-ecommerce\versions\2"

spark = SparkSession.builder.appName("Olist").getOrCreate()

df_olist_orders = spark.read.csv(path=f"{path}/olist_orders_dataset.csv", header=True, inferSchema=True)
df_olist_order_items = spark.read.csv(path=f"{path}/olist_order_items_dataset.csv", header=True, inferSchema=True)
df_olist_products = spark.read.csv(path=f"{path}/olist_products_dataset.csv", header=True, inferSchema=True)
df_olist_customers = spark.read.csv(path=f"{path}/olist_customers_dataset.csv", header=True, inferSchema=True)

print("orders")
df_olist_orders.printSchema()
print("order_items")
df_olist_order_items.printSchema()
print("products")
df_olist_products.printSchema()
print("customers")
df_olist_customers.printSchema()

# 1. Escolher as 100 primeiras linhas para validar os dados

# Usando SQL
df_olist_products.createGlobalTempView("products")
df_olist_orders.createGlobalTempView("orders")
df_olist_order_items.createGlobalTempView("order_items")
df_olist_customers.createGlobalTempView("customers")
'''spark.sql("SELECT product_id AS id_do_produto, \
product_category_name AS nome_categoria_produto \
FROM global_temp.products \
LIMIT 100"
).show(100)
'''

# 2. Tarefa: Na tabela olist_order_items, filtre apenas vendas onde o preço 
# seja maior que 500 reais OU o frete seja igual a 0. Exclua categorias para simular um filtro.
'''
spark.sql("SELECT *   \
FROM global_temp.order_items \
WHERE (price > 500 OR freight_value = 0) \
").show()

spark.sql("SELECT * FROM global_temp.products  \
WHERE (product_length_cm > 20 OR product_weight_g > 200)  \
AND product_category_name NOT IN ('artes', 'perfumaria', 'bebes')").show()
'''

# 3. Calcule a soma total da coluna price, a média de valor de frete e 
# qual foi a venda mais cara registrada no histórico.
'''
spark.sql("SELECT SUM(price) as soma_preco,  \
MEAN(freight_value) as media_frete,  \
MAX(price) as venda_cara \
FROM global_temp.order_items").show()
'''

# 4. Agrupe por estado e conte quantos clientes existem. 
# Mostre apenas estados com mais de 1000 clientes.
'''
spark.sql("SELECT DISTINCT(customer_state), COUNT(DISTINCT customer_id) as customer_count \
FROM global_temp.customers \
GROUP BY(customer_state) \
HAVING COUNT(customer_state)>1000 \
ORDER BY customer_count DESC").show()
'''
# 5. Agrupe por product_id, some o valor das vendas, e ordene do maior para o menor valor.
# WHERE é um filtro de linha, filtra antes do group by
# É bom quando a informação ja ta na tabela original
# HAVING é um filtro de grupos, filtra depois do group by.
# É bom quando a informação acabou de ser obtida, pela querry ou group by
'''
spark.sql("SELECT product_id, sum(price) as soma_por_produto  \
FROM global_temp.order_items \
GROUP BY product_id  \
ORDER BY soma_por_produto DESC").show()

'''

# 6. Faça um INNER JOIN entre olist_orders e olist_customers usando o customer_id.
'''
spark.sql("SELECT * FROM global_temp.orders \
INNER JOIN global_temp.customers \
ON global_temp.orders.customer_id = global_temp.customers.customer_id").show()
'''
# 7. Crie uma nova coluna chamada chave_busca. Ela deve ser a concatenação da cidade 
# do cliente com o estado, tudo em letras maiúsculas e sem espaços no início/fim.
'''
spark.sql("SELECT TRIM(UPPER(CONCAT(global_temp.customers.customer_city, ' - ', global_temp.customers.customer_state))) as chave_busca \
FROM global_temp.customers").show()
'''
# 8. Faça um KPI de dias médios entre a entrega e o começo do pedido
# agrupado por mes
'''
spark.sql("SELECT ROUND(AVG(DATEDIFF(global_temp.orders.order_delivered_customer_date, global_temp.orders.order_purchase_timestamp)), 2) as dias_medios_p_entrega, \
DATE_TRUNC('MONTH', global_temp.orders.order_purchase_timestamp) as mes_ref \
FROM global_temp.orders \
GROUP BY mes_ref \
ORDER BY mes_ref").show()
'''
# 9. Crie uma coluna nova categoria_frete.
# Se frete < 10: "Barato"
# Se frete entre 10 e 50: "Normal"
# Se frete > 50: "Caro"
'''
spark.sql("SELECT freight_value, \
 CASE \
   WHEN global_temp.order_items.freight_value < 10 THEN 'Barato' \
   WHEN global_temp.order_items.freight_value >= 10 AND global_temp.order_items.freight_value <= 50 THEN 'Normal' \
   ELSE 'Caro' \
 END AS categoria_frete \
 FROM global_temp.order_items").show()
'''

# 10. Algumas datas de entrega estão nulas (pedidos não entregues). 
# Substitua as datas nulas pela data de hoje (simulando "ainda em aberto")
'''
spark.sql("SELECT COALESCE(order_delivered_customer_date, CURRENT_TIMESTAMP()) as data_tratada \
 FROM global_temp.orders").show()
'''
# 11. De um número (1, 2, 3...) para cada repetição do mesmo pedido. 
# Filtre apenas onde o número é 1.
'''
spark.sql("WITH pedidos_rank AS( \
 SELECT \
    order_id, \
    order_purchase_timestamp, \
    ROW_NUMBER() OVER( \
      PARTITION BY order_id \
      ORDER BY order_purchase_timestamp DESC" \
      " \
    ) as n_repeticao \
  FROM global_temp.orders \
) \
SELECT * FROM pedidos_rank \
WHERE n_repeticao = 1").show()
'''

# 12. Para cada cliente, calcule quanto tempo (em dias) passou entre a compra atual e a compra anterior dele.
'''
spark.sql("WITH historico_cliente AS ( \
   SELECT c.customer_unique_id, \
          o.order_id, \
          o.order_purchase_timestamp, \
          LAG(o.order_purchase_timestamp) OVER ( \
             PARTITION BY c.customer_unique_id \
             ORDER BY o.order_purchase_timestamp \
          ) as data_compra_anterior \
   FROM global_temp.customers c \
   JOIN global_temp.orders o ON c.customer_id = o.customer_id \
 ) \
 SELECT *, DATEDIFF(order_purchase_timestamp,data_compra_anterior) as dias_desde_compra_ant \
 FROM historico_cliente \
 WHERE data_compra_anterior IS NOT NULL \
 ").show()
'''
# 13. Descobrir qual a categoria de produto mais vendida em cada estado brasileiro.
# linkamos oi -> p
# linkas oi -> o -> c
'''
spark.sql("WITH pega_dados AS ( \
  SELECT c.customer_state, \
         p.product_category_name, \
         COUNT(oi.product_id) as n_vendas \
  FROM global_temp.order_items oi \
  JOIN global_temp.products p ON p.product_id = oi.product_id \
  JOIN global_temp.orders o ON o.order_id = oi.order_id \
  JOIN global_temp.customers c ON o.customer_id = c.customer_id \
  GROUP BY 2, 1 \
), \
 ranqueia AS ( \
 SELECT *, RANK() OVER (PARTITION BY customer_state ORDER BY n_vendas DESC) as rnk  \
 FROM pega_dados\
 )\
SELECT * from ranqueia \
WHERE rnk = 1 \
").show()
'''
'''
# 14. Calcule o faturamento acumulado da Olist ao longo do tempo (dia a dia).
spark.sql(" WITH somma AS ( \
              SELECT SUM(oi.price) as soma_valores, \
                     DATE_TRUNC('DAY', o.order_purchase_timestamp) as dias\
              FROM global_temp.order_items oi  \
              JOIN global_temp.orders o ON oi.order_id = o.order_id  \
              GROUP BY dias\
) \
SELECT dias, ROUND(SUM(soma_valores) OVER (ORDER BY dias ASC), 2) as soma_acumulada FROM somma ORDER BY dias\
").show()
'''
spark.stop()