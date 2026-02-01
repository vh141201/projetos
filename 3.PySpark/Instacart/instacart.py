'''
"Victor, quanto tempo nossos clientes levam para voltar e fazer a segunda compra depois que conhecem o app?"

"Me tire uma lista dos nossos 10 melhores clientes, mas só aqueles que realmente compram cestas cheias, com mais de 5 itens."

"Preciso separar nossos clientes em três grupos: os novatos, os que compram de vez em quando e os nossos fãs número um. Qual o faturamento de cada grupo?"

"Gostaria de ver, para cada pedido de um cliente, quanto tempo ele demorou para fazer a compra seguinte."

"Quero entender o comportamento no carrinho: me mostre como o valor total vai crescendo à medida que o cliente adiciona cada item."

"Quais são os 3 produtos campeões de recompra em cada corredor do nosso mercado?"

"Como estão as nossas vendas por hora? Mas não me dê o número seco, use uma média das últimas 3 horas para tirar os picos e vales."

"Existe algum produto de laticínios que o pessoal que compra bebida alcoólica nunca leva junto?"

"Nossos clientes estão sendo fiéis aos dias de costume? Compare se o dia que ele comprou hoje é o mesmo que ele costuma comprar sempre."

"Qual o intervalo médio entre compras para cada corredor? Considere que quem está comprando pela primeira vez não tem tempo de espera."

"Como nossas vendas se dividem entre madrugada, manhã, tarde e noite? Onde está o nosso grosso?"

"Quais produtos são as nossas 'portas de entrada'? Ou seja, qual o primeiro item que as pessoas colocam no carrinho com mais frequência?"

"Tivemos um crescimento de pedidos entre o domingo e a segunda-feira? Qual foi a porcentagem?"

"Qual o tamanho médio das últimas 3 cestas de cada um dos nossos clientes?"

"Quero saber qual produto é 'fogo de palha' em cada departamento: aquele que o cliente compra uma vez e nunca mais volta a levar."

"Me dê um alerta de quem está prestes a nos abandonar: clientes que estão demorando muito mais do que o normal deles para voltar."

"Dê uma checada se tem algum pedido perdido no nosso sistema de vendas que não aparece no nosso registro principal de ordens."

"Monte um 'tabelão' único com todas as informações de produtos e vendas para eu conseguir analisar tudo em um lugar só."

"Para cada cliente, qual foi o dia e o código do pedido em que ele fez a maior compra da vida dele aqui?"

"Quais clientes são os mais ecléticos e compram produtos de mais de 5 departamentos diferentes em uma única tacada?"'''


# Tabelas: pedidos, produtos, clientes, departamentos e itens_do_pedido.

from pyspark.sql import SparkSession
import kagglehub

#  Dataset do kaggle
path = kagglehub.dataset_download("psparks/instacart-market-basket-analysis")

spark = SparkSession.builder.getOrCreate()
print(path)

df_instacart_pedidos = spark.read.csv(path=f"{path}/orders.csv", header=True, inferSchema=True)
df_instacart_produtos = spark.read.csv(path=f"{path}/products.csv", header=True, inferSchema=True)
df_instacart_ilhas = spark.read.csv(path=f"{path}/aisles.csv", header=True, inferSchema=True)
df_instacart_departamentos = spark.read.csv(path=f"{path}/departments.csv", header=True, inferSchema=True)
df_instacart_pedidos_antes = spark.read.csv(path=f"{path}/order_products__prior.csv", header=True, inferSchema=True)

print("pedidos")
df_instacart_pedidos.printSchema()
print("produtos")
df_instacart_produtos.printSchema()
print("ilhas")
df_instacart_ilhas.printSchema()
print("departamentos")
df_instacart_departamentos.printSchema()
print("pedidos_antes")
df_instacart_pedidos_antes.printSchema()

df_instacart_pedidos.createGlobalTempView("pedidos")
df_instacart_produtos.createGlobalTempView("produtos")
df_instacart_ilhas.createGlobalTempView("ilhas")
df_instacart_departamentos.createGlobalTempView("departamentos")
df_instacart_pedidos_antes.createGlobalTempView("pedidos_antes")

