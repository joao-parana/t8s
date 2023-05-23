# Design Pattern used 

As transformações executadas na `TimeSerie` são implementadas usando o padrão de projeto `Strategy`.

![strategy design pattern](img/strategy-pattern.png)

1. O `Context` mantém uma referência a uma das estratégias concretas e se comunica com esse objeto
apenas por meio da interface da estratégia.
2. A interface `Strategy` é comum a todas as estratégias concretas. Ele declara um método que o
contexto usa para executar uma estratégia.
3. `ConcreteStrategies` implementam diferentes variações de um algoritmo que o contexto usa.
4. O `Context` chama o método da estratégia que deve executar a lógica de negócios. Normalmente,
as estratégias têm permissão para fazer seu trabalho livremente, sem se preocupar com os dados
de contexto. No entanto, o padrão também permite que você passe alguns dados de contexto, se
necessário, em resumo, o contexto chama o método de execução no objeto de estratégia vinculado,
cada vez que precisa executar o algoritmo. O contexto não sabe com que tipo de estratégia ele
trabalha ou como o algoritmo é executado.
5. O `Client` cria um objeto de estratégia (ou obtem de um registro) e passa-o para o contexto.
O cliente deve selecionar a estratégia que melhor se adapta à sua necessidade atual. Como
alternativa, você pode deixar o contexto escolher a estratégia com base nos detalhes de
configuração, lógica de negócios, tipo de banco de dados ou qualquer outro parâmetro passado. Simplificando: o `Client` cria um objeto de estratégia específico e o passa para o contexto. O
contexto expõe um setter que permite aos clientes substituir a estratégia associada ao contexto
em tempo de execução.

**Notas:**

- Os clientes devem estar cientes das diferenças entre as estratégias para poder selecionar a mais adequada
- Muitas linguagens de programação modernas têm suporte de tipo funcional que permite implementar diferentes versões de um algoritmo dentro de um conjunto de funções anônimas. Em seguida, você poderia usar essas funções exatamente como usaria os objetos de estratégia.
- O padrão Strategy é muito comum em Python. Muitas estruturas e bibliotecas Python usam-o para implementar
diferentes tipos de coleções e contêineres.

**Relações com outros Padrões de Projeto**

- `Bridge`, `State`, `Strategy` (e até certo ponto `Adapter`) têm estruturas muito semelhantes. De fato, todos esses padrões são baseados na composição, que é delegar trabalho a outros objetos. No entanto, todos eles resolvem problemas diferentes. Um padrão não é apenas uma receita para estruturar seu código de uma maneira específica. Ele também pode comunicar a outros desenvolvedores o problema que o padrão resolve.

- `Command` e `Strategy` podem parecer semelhantes porque você pode usar ambos para parametrizar um objeto com alguma ação. No entanto, eles têm intenções muito diferentes. Você pode usar `Command` para converter qualquer operação em um objeto. Os parâmetros da operação tornam-se campos desse objeto. A conversão permite adiar a execução da operação, colocá-la em fila, armazenar o histórico de comandos, enviar comandos para serviços remotos, etc. Por outro lado, `Strategy` geralmente descreve maneiras diferentes de fazer a mesma coisa, permitindo que você troque esses algoritmos em uma única classe de contexto.

- O `Decorator` permite que você mude a aparência de um objeto, enquanto o Strategy permite que você mude as entranhas.

- O `Template Method` é baseado em herança: ele permite que você altere partes de um algoritmo estendendo essas partes em subclasses. A estratégia é baseada na composição: você pode alterar partes do comportamento do objeto, fornecendo-lhe diferentes estratégias que correspondem a esse comportamento. `Template Method`  funciona no nível da classe, então é estático. A `Strategy` funciona no nível do objeto, permitindo que você alterne os comportamentos em tempo de execução.

- O padrão `State` pode ser considerado como uma extensão da Estratégia. Ambos os padrões são baseados em composição: eles mudam o comportamento do contexto delegando algum trabalho a objetos auxiliares. A estratégia torna esses objetos completamente independentes e inconscientes uns dos outros. No entanto, State não restringe dependências entre estados concretos, deixando-os alterar o estado do contexto à vontade.

Neste caso particular a classe `TSBuilder` é o Contexto. Ela poderia se chamar TSLoader ou TSReader, mas
o nome `TSBuilder` foi escolhido para enfatizar que a classe é responsável por construir a `TimeSerie` e não apenas ler ou carregar do arquivo, já que nem sempre a origem do dado será o _filesystem_.
