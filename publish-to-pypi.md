### Publicando o pacote na repositório https://pypi.org/project/t8s/

Após executar os testes fazer:

```bash
hatch build
```

A resposta será algo pareceido com isto abaixo:

```txt
[sdist]
dist/t8s-0.1.0.tar.gz

[wheel]
dist/t8s-0.1.0-py3-none-any.whl
```

Veja a credencial em `$HOME/.pypirc` na seção `[pypi]` e copie a senha para a área de transferência.
Depois execute o comando abaixo e ao ser solicitado informar a `credentials` faça o PASTE. 

```
hatch publish
```

Veja um exemplo:

```txt
Enter your username: __token__
Enter your credentials:************ . . .
dist/t8s-0.1.0.tar.gz ... success
dist/t8s-0.1.0-py3-none-any.whl ... success

[t8s]
https://pypi.org/project/t8s/0.1.0/
```
