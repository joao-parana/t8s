# Design Pattern used: Composite

As composições de objetos `TimeSerie` gerando Datasets mais complexos são implementadas usando
o padrão de projeto `Composite`.

![composite design pattern](img/composite-pattern.png)

1. A interface `Component` descreve operações que são comuns para elementos simples ou complexos da
   composição.
2. `Leafs` são elementos básicos da composição que não têm filhos. Normalmente, os objetos `Leafs`
   executam algum trabalho real, enquanto os objetos `Composite` apenas delegam para seus filhos.
3. Normalmente, os objetos `Composite` armazenam uma coleção de referências para seus filhos
   funcionando como um Container. Em alguns casos, essa coleção pode ser uma lista simples ou
   um dicionário ordenado. No entanto, às vezes pode ser mais conveniente usar uma lista encadeada,
   que impõe a ordem em que os elementos são percorridos.
4. O `Client` funciona com todos os elementos através da interface `Component`. Dessa forma, o
   `Client` pode trabalhar com objetos simples ou complexos da mesma maneira.


**Notas:**

- Neste _design pattern_ podemos trabalhar com estruturas de árvore complexas de forma mais conveniente,
  usando o polimorfismo e a recursão a nosso favor.
- Princípio Aberto/Fechado. Podemos introduzir novos tipos de elementos no aplicativo sem quebrar o
  código existente, que no _pattern_ funciona com a árvore de objetos.
- Liskov Substitution Principle. Podemos trabalhar com objetos complexos da mesma forma que com
  objetos simples da composição. Ou seja, podemos ignorar a diferença entre objetos compostos e
  individuais.

**Relações com outros Padrões de Projeto:**

- Podemos usar o _pattern_ `Visitor` para percorrer elementos compostos e executar uma dada
  operação em uma árvore composta inteira.
- Podemos usar o _pattern_ `Iterator` para percorrer elementos compostos.
- Podemos usar o _pattern_ `Decorator` para estender a funcionalidade de classes individuais
  (não necessariamente relacionadas à composição). `Composite` e `Decorator` têm diagramas
  de estrutura semelhantes, pois ambos contam com composição recursiva para organizar um 
  número arbitrário de objetos. Um Decorator é como um Composite, mas tem apenas um componente
  filho. Há outra diferença significativa: o Decorator adiciona responsabilidades adicionais
  ao objeto envolvido, enquanto o Composite apenas "sumariza" os resultados de seus filhos. No
  entanto, os padrões também podem cooperar: você pode usar o Decorator para estender o
  comportamento de um objeto específico na árvore Composite.
- Podemos usar o _pattern_ `Chain of Responsibility` para deixar os componentes da composição
  responderem às solicitações recursivamente. Nesse caso, quando um componente folha recebe
  uma solicitação, ele pode passá-la pela cadeia de todos os componentes pais até a raiz da
  árvore de objetos.
- Podemos usar o _pattern_ `Flyweight` para reduzir o uso da memória de objetos complexos da
  composição.
- Podemos usar o _pattern_ `Memento` para capturar o estado da composição e restaurá-lo
  posteriormente.
- Podemos usar o _pattern_ `Command` para converter solicitações em objetos independentes que
  contêm toda a informação sobre a solicitação e registrar os objetos `Command` na Gestão de
  Proveniência.
- Podemos usar o _pattern_ `Mediator` para deixar os componentes da composição se comunicarem
  entre si, caso exista tal necessidade.
- Podemos usar o _pattern_ `Observer` para que os componentes da composição possam receber
  notificações de eventos importantes.
- Podemos usar o _pattern_ `Prototype` para copiar objetos compostos existentes sem fazer
  seu código depender de suas classes. Projetos que fazem uso intenso de `Composite` e `Decorator`
  geralmente podem se beneficiar do uso de `Prototype` pois aplicar o padrão permite clonar
  estruturas complexas em vez de reconstruí-las do zero.
- Podemos usar o _pattern_ `Singleton` junto com o `Composite`. Este seria um singleton
  de uma árvore inteira, não apenas de uma única classe.
- Podemos usar o _pattern_ `Builder` para construir árvores de objetos complexos passo a passo.
  Diferentemente de outros padrões de criação, o `Builder` permite produzir produtos diferentes
  usando o mesmo processo de construção.
- Podemos usar o _pattern_ `Abstract Factory` para produzir várias famílias de objetos compostos.
