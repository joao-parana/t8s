# behave

O módulo behave é um framework de testes orientado a comportamento (BDD - Behavior Driven Development) para uso em projetos Python. Ele permite que você escreva testes em uma linguagem natural e legível para humanos, que são então traduzidos em código Python executável. O behave é baseado na linguagem `Gherkin`, que é uma linguagem de especificação de comportamento que usa gramática propria com palavras-chave como "Given", "When" e "Then" para descrever o comportamento esperado de um sistema.

O behave é útil para testar sistemas complexos e permite escrever testes em uma linguagem que é facilmente compreensível para todas as partes interessadas, incluindo desenvolvedores, gerentes de projeto, _product owners_ e clientes. Ele também é mais abrangente e ajuda a garantir que o código seja testado em termos de comportamento esperado, em vez de apenas testar a funcionalidade individual de cada componente.

Com o behave temos:

- Testes mais legíveis: os testes escritos em Gherkin são fáceis de ler e entender, mesmo para pessoas que não são desenvolvedores.

- Testes mais abrangentes: os testes de comportamento testam o sistema como um todo, em vez de apenas testar componentes individuais.

- Testes mais fáceis de manter: os testes de comportamento são menos propensos a quebrar quando o código é alterado, pois eles se concentram no comportamento do sistema, em vez de detalhes de implementação.

- Integração com outras ferramentas: o behave pode ser integrado com outras ferramentas de teste, como o `pytest``, para fornecer uma suíte de testes completa.

No behave a gente define cenários usando a linguagem a linguagem `Gherkin` e o framework faz o _binding_ com o código Python que implementa os passos definidos nos cenários. Estes passos são anotados com decoradores especiais (@given, @when, @then, ...) que são reconhecidos pelo framework behave.

Veja abaixo um exemplo extremamente simples, apenas para mostrar o conceito:

Com a definição Gherkin do cenário :

```gherkin
# language: pt

Funcionalidade: Testando o behave

  Cenário: Testando o behave
    Dado que temos o behave instalado
    Quando implementamos 10 testes
    Então o behave vai testar pra gente!
```

O _binding_ em Python fica assim:

```python
@given(u'que temos o behave instalado')
def step_impl(context):
    logger.info('STEP: Given que temos o behave instalado')


@when(u'implementamos {qty} testes')
def step_impl(context, qty):
    logger.info(f'STEP: When implementamos {qty} testes')


@then(u'o behave vai testar pra gente!')
```

O framework passa os dados variáveis para os passos usando o parâmetro `context` que é um objeto do tipo `Context` que pode ser usado para compartilhar dados entre os passos. Além disso na step `'implementamos {qty} testes'` o runtime cria a variavel `qty` localmente, que pode ser usada no código Python.

Veja a definição da [gramatica Gherkin para Português](https://github.com/cucumber/gherkin/blob/main/gherkin-languages.json#L2698)

Podemos executar o teste de uma feature especifica usando o comando como mostrado abaixo:

```bash
rm logs/timeseries.log
python3 -m behave --logging-level INFO --no-capture --no-capture-stderr --no-skipped \
        silly-features/03.example.feature; cat logs/timeseries.log
```

O behave usa a classe `behave.runner:Runner` para executar os testes de aceitação.
