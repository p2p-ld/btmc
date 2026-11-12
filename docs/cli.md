# CLI

Install with the `cli` extra:

```shell
python -m pip install 'btmc[cli]'
```

`btmc mount` also needs the `mount` extra and FUSE, see [Mounting](./filesystem.md#mounting).

```{eval-rst}
.. click:: btmc.cli.main:main
    :prog: btmc
    :nested: full
```
