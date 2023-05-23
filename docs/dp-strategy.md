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
