# O Husky e o git

`file: t8s/husky/README.md`

O Husky é uma ferramenta que ajuda a facilitar o uso do Git em projetos de desenvolvimento de software. Ele permite que você defina ganchos (hooks) personalizados que são executados automaticamente em determinados eventos do Git, como antes de um commit ou antes de um push. Esses ganchos podem ser configurados para executar tarefas específicas, como executar testes, executar linters de código, formatar arquivos ou qualquer outra ação personalizada que você deseje realizar antes de confirmar ou enviar suas alterações.

Ao utilizar o Husky, você pode automatizar várias tarefas do Git e garantir que certas ações sejam realizadas antes de confirmar suas alterações, garantindo assim a integridade do seu código. Ele ajuda a manter um fluxo de trabalho consistente e evita a introdução de código com problemas no repositório.

Além disso, o Husky possui uma integração fácil com outras ferramentas populares, como o ESLint (para linting de código JavaScript) e o Prettier (para formatação de código). Isso permite que você automatize a execução dessas ferramentas durante o processo de commit, por exemplo, para garantir que seu código esteja sempre formatado corretamente e de acordo com as convenções definidas.

Em resumo, o Husky torna o uso do Git mais fácil, automatizando tarefas importantes e garantindo a consistência do código em um projeto de desenvolvimento de software. Ele pode ser especialmente útil quando usado em conjunto com outras ferramentas de linting e formatação de código.

## Usando com Python

Podemos usar o Husky com ferramentas como PyRight e pytest para automatizar tarefas relacionadas ao Git em projetos Python.

Para configurar o Husky para trabalhar com essas ferramentas, você precisará seguir os seguintes passos:

- Instalar o Husky: Você pode instalá-lo através do gerenciador de pacotes npm, executando o seguinte comando no terminal:

npm install husky --save-dev

- Configurar os ganchos (hooks) do Husky: No diretório raiz do seu projeto, você precisará criar um arquivo chamado .huskyrc (ou .huskyrc.json ou .huskyrc.js, dependendo da sua preferência), onde definirá os ganchos desejados. Por exemplo, para configurar o Husky para executar o PyRight antes de cada commit, você pode adicionar o seguinte código ao arquivo .huskyrc:

```json

{
  "hooks": {
    "pre-commit": "pyright"
  }
}
```

Instalar as ferramentas relacionadas ao Python: Certifique-se de ter instalado o PyRight e o pytest em seu ambiente Python. Você pode instalá-los usando o pip, por exemplo:

```bash
    pip install pyright pytest
```

- Configurar as ferramentas Python: Dependendo da ferramenta Python que você deseja usar com o Husky, pode ser necessário configurá-la separadamente. Por exemplo, para o **pytest**, você pode criar um arquivo de configuração `pytest.ini` ou `pyproject.toml` com as configurações adequadas para sua suíte de testes.

Após seguir esses passos, o Husky estará configurado para executar o PyRight antes de cada commit e você poderá adicionar outros ganchos para executar o pytest ou outras ferramentas de sua escolha.

Lembrando que você pode personalizar os ganchos do Husky de acordo com suas necessidades, executando várias tarefas ou combinações de ferramentas.

## A relação com o ecossistema Commitizen

A ferramenta `cz-cli` faz parte do ecossistema do Commitizen, que é um conjunto de ferramentas e convenções para padronizar as mensagens de commit em um repositório Git. 

O Husky, por sua vez como vimos acima, é uma ferramenta separada que permite automatizar tarefas do Git, incluindo a execução de ganchos personalizados.

A relação entre o `cz-cli` e o `Husky` está no fato de que eles podem ser usados em conjunto para facilitar o uso das convenções do Commitizen e garantir que as mensagens de commit sigam um formato específico.

Aqui está como eles se relacionam:

- Commitizen e cz-cli: O Commitizen é uma convenção que estabelece um padrão para as mensagens de commit, promovendo uma estrutura consistente e legível. O cz-cli é uma ferramenta do Commitizen que fornece uma interface de linha de comando interativa para ajudar os desenvolvedores a criar mensagens de commit no formato adequado. Ele orienta o usuário a preencher campos específicos, como tipo de alteração, escopo e descrição, seguindo as convenções do Commitizen.

- Husky e ganchos de commit: O Husky permite que você defina ganchos personalizados do Git, como o pre-commit ou commit-msg. Com o Husky configurado corretamente, você pode adicionar um gancho de commit que dispara o cz-cli antes de cada commit. Isso significa que, quando você executar um commit, o cz-cli será acionado e você será guiado a preencher as informações necessárias para uma mensagem de commit adequada.

Dessa forma, o Husky atua como um mecanismo para executar o cz-cli automaticamente durante um commit, garantindo que as mensagens de commit sigam as convenções definidas pelo Commitizen.

Em resumo, o cz-cli do Commitizen é uma ferramenta para criar mensagens de commit padronizadas, enquanto o Husky é uma ferramenta para automatizar tarefas do Git. Ao combiná-los, você pode automatizar a geração de mensagens de commit com o formato apropriado definido pelo Commitizen usando o Husky como um gatilho para executar o cz-cli.

Veja abaixo um exemplo de chamada para `git cz`  em vez de `git commit` que direciona o desenvolvedor a escrever uma mensagem de `commit` em conformidade com os padrões.

![add commit](https://raw.githubusercontent.com/joao-parana/t8s/master/husky/add-commit.png)



