# Desafio_Raizen.
Repositório para anexar e descrever a solução para o desafio proposto pela Raízen.



# Considerações iniciais.
O desafio propõe um arquivo no formato XLS, no qual será necessário resgatar e extrair os dados internos de suas tabelas pivot
para assim processar os dados segundo indicado no Desafio.
A solução consiste em transformar o arquivo para o formato XLSX. Para que assim seja possível acessar seus dados brutos.
com a solução em mente, foram abordados dois possíveis caminhos:

# Caminhos.
O primeiro e oficial, denominada pela DAG: "desafio_raizen_1", consiste na preparação e subida do ambiente docker com airflow,
e utilização do arquivo pré resgatado da origem proposta pelo desafio.
Onde para realizar a transformação de formatos, foi utilizado o aplicativo LibreOffice, que através do comando:
"C:\Program Files\LibreOffice\program\soffice.exe" --convert-to xlsx "C:\local_do_arquivo\file.xls" -outdir "C:\saida_do_arquivo"
retorna o arquivo formatado para o padrão desejado.
Com o arquivo em mãos, basta apenas aloca-lo na pasta "diretorio" dentro do diretorio destinado aos componentes docker.
# 
O segundo caminho(não-finalizado) denominado pela DAG: "desafio_raizen_teste_DO" consiste em adquirir e transformar o arquivo através de codificação Python.
Para isso foram utilizadas algumas biblíotecas como: requests, os e sys.
essa transformação se daria através de um DockerOperetor com uma imagem do LibreOffice.
após rodar o comando, o DockerOperetor não é capaz de acessar a pasta de destino por falta de permissões.
Para isso decidi empacotar o arquivo em torno de um serviço (acessível via TCP) que usa uma imagem socat e expõe o serviço no tcp://localhost:1234 dentro do composer.
No entanto o DockerOperetor não conseguiu acesso ao caminho de rede local. (Creio que seja necessário apenas uma configuração para ajustar corretamente as conexões entre os caminhos compartilhados)

# Desenvolvimento da solução.
Com o arquivo transformado, a solução se baseia em adquirir as sheets do XLSX e gravar o resultado em um DataFrame, para que assim seja possível finalizar as transformações necessárias
e formatar o codigo para o AirFlow.
#
O aplicação deve retornar os seguintes resultados:

# Pipeline


![Airflow](https://user-images.githubusercontent.com/86423797/162024150-e88d4e2c-7fc0-4098-961a-234e01cd0b60.png)

# Arquivos
diesel_by_uf_and_type

![resutado](https://user-images.githubusercontent.com/86423797/162025865-f0112e30-07f7-4174-bbf1-a3d698673b7a.png)

#

oil_derivative_fuels_by_uf_and_product

![resultado_2](https://user-images.githubusercontent.com/86423797/162026757-0b666a95-38b2-4c3b-bb9d-198678bef608.png)


