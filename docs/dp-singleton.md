# Design Pattern used: Singleton

A configuração de Logger foi implementada usando o padrão de projeto `Singleton`.

![singleton design pattern](img/singleton-pattern.png)

1. A classe Singleton declara o método estático getInstance que retorna a mesma instância de sua
   própria classe.
2. O construtor do Singleton deve estar oculto no código do cliente. Chamar o método getInstance()
   deve ser a única maneira de obter o objeto Singleton.

**Notas:**

O _pattern_ `Singleton` é uma forma resiliente de compartilhar recursos entre diferentes partes do
programa garantindo a existência de uma e apenas uma instância.



**Relações com outros Padrões de Projeto**

- Uma classe `Facade` geralmente pode ser transformada em um `Singleton`, pois um único objeto de
  fachada é suficiente na maioria dos casos. Podemos usar o _pattern_ `Singleton` junto com o
  _pattern_ `Facade` quando precisamos de acesso a um único objeto de fachada para um determinado
  contexto de solicitação, por exemplo, em aplicativos multithread.
- O `Flyweight` se pareceria com o `Singleton` se você de alguma forma conseguisse reduzir todos os
  estados compartilhados dos objetos a apenas um objeto flyweight. Mas há duas diferenças fundamentais
  entre esses padrões: Deve haver apenas uma instância Singleton, enquanto uma classe Flyweight
  pode ter várias instâncias com diferentes estados intrínsecos. O objeto `Singleton` pode ser mutável.
  Os objetos `Flyweight` são imutáveis.
- `Abstract Factories`, `Builders` e `Prototypes` podem ser implementados como `Singletons`.
